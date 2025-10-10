#!/usr/bin/env python3
"""
Robot WS Client (Custom Protocol)

一个通用的 WebSocket 客户端，适配自定义（私有）协议的智能体。
支持：
- 长连接与自动重连（指数退避）
- 心跳（依赖 websockets 的 ping/pong）
- 文本/JSON 消息接收与回调分发
- 线程托管 asyncio 事件循环，提供同步友好的 start/stop/send 接口
"""

import json, os, threading, time
import asyncio, websockets
import urllib.request, urllib.error
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK
from typing import Optional, Callable, Dict, Any
from fastmcp.utilities.logging import get_logger

logger = get_logger(__name__)


class RobotClient:
    """
    自定义协议 Robot 的 WebSocket 客户端。

    Args:
        url: WebSocket 服务端地址，例如 ws://host:port/path 或 wss://...
        reconnect: 是否在断开后自动重连
    """

    # 固定常量（如需调整可直接改这里）
    HEARTBEAT_INTERVAL: int = 5
    RECONNECT_COOLDOWN: int = 3
    DEFAULT_HEADERS: Dict[str, str] = {}
    # 快速重连信号的最小触发间隔（秒），避免高频信号导致刷屏

    SOURCE = 'im-proxy'

    def __init__(self, host: str, reconnect: bool = True, on_event: Optional[Callable[[str, Dict[str, Any]], None]] = None,
                 worker_id: Optional[str] = None, home_path: Optional[str] = None) -> None:
        uri = f'socket?source={self.SOURCE}'
        self.base_url = f"http://{host}"
        self.ws_url = f"ws://{host}/{uri}"
        self.reconnect = reconnect
        self.headers = self.DEFAULT_HEADERS
        self.heartbeat_interval = self.HEARTBEAT_INTERVAL

        # Optional runtime configs for starting conversation via HTTP
        self.worker_id = worker_id or os.getenv("FEISHU_WORKER_ID")
        self.home_path = home_path or os.getenv("FEISHU_HOME_PATH")

        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._stop = threading.Event()
        self._send_lock = threading.Lock()
        # 外部快速重连信号：用于从退避等待中提前唤醒
        self._reconnect_signal = threading.Event()
        # 日志与信号节流
        self._last_reconnect_ts: float = 0.0
        self._last_error_log_ts: float = 0.0
        self._last_close_log_ts: float = 0.0
        self._connected: bool = False
        self._on_event = on_event

    # ---------- Public API ----------
    def start(self) -> None:
        """启动后台线程并建立长连接（异步运行）。"""
        if self._thread and self._thread.is_alive():
            logger.info("[Robot] already running")
            return

        self._stop.clear()
        self._loop = asyncio.new_event_loop()
        def runner():
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._run())
        self._thread = threading.Thread(target=runner, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """停止长连接并关闭线程。"""
        if not self._loop:
            return
        self._stop.set()
        # 关闭连接
        if self._ws:
            try:
                asyncio.run_coroutine_threadsafe(
                    self._ws.close(), self._loop,
                ).result(timeout=5)
            except Exception:
                pass
        # 结束事件循环
        try:
            self._loop.call_soon_threadsafe(self._loop.stop)
        except Exception:
            pass
    
    def get_intent(self, content: str, uploads: Optional[list] = [], session: str = "feishu-bot") -> Optional[Dict[str, Any]]:
        """
        意图识别（HTTP POST /api/intent）。

        请求体包含：
        - content: 任务内容（去除首尾空白）
        - session: 会话标识，默认 "feishu-bot"

        返回：服务端 JSON 响应（dict），示例结构：
        {
            "intent": string,
            "taskid": string,
            "worker": string,
            "emoji": string,
            "message": string
        }
        失败返回 {"errmsg": ...}
        """
        url = f"{self.base_url}/api/intent"
        body: Dict[str, str] = {
            "source": self.SOURCE,
            "content": content,
            "session": session,
            "uploads": uploads,
        }

        logger.info(f"intent request: content='{content}, uploads={uploads}'")
        payload = json.dumps(body, ensure_ascii=False)
        req = urllib.request.Request(
            url=url, data=payload.encode("utf-8"), method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            res = urllib.request.urlopen(req, timeout=90)
            code = getattr(res, "status", res.getcode())
            if code < 200 or code >= 300:
                return {"errmsg": f"Req err: {getattr(res, 'reason', 'unknown')}"}
            data = res.read().decode("utf-8")
            result = json.loads(data)
        except Exception as e:
            result = {"errmsg": str(e)}
        logger.info(f"intent response: {result}")
        # 如果意图识别成功，且长连接断开，则尝试触发快速重连
        try:
            if not result.get("errmsg"):
                self._ensure_ws_connected()
        except Exception:
            pass
        return result

    def send_json(self, data: Dict[str, Any]) -> bool:
        """发送 JSON 消息"""
        try:
            payload = json.dumps(data, ensure_ascii=False)
            return self.send_text(payload)
        except Exception as e:
            logger.error(f"send_json encode error: {e}")
            return False

    def send_text(self, text: str) -> bool:
        """发送文本消息"""
        if not self._loop or not self._ws:
            logger.warning("not connected")
            return False
        with self._send_lock:
            fut = asyncio.run_coroutine_threadsafe(
                self._ws.send(text), self._loop,
            )
            try:
                fut.result(timeout=5)
                return True
            except Exception as e:
                logger.error(f"send_text error: {e}")
                return False

    # ---------- Internal ----------
    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                # 建立连接
                logger.info(f"connecting to {self.ws_url}...")
                self._ws = await websockets.connect(
                    self.ws_url, max_size=8 * 1024 * 1024,
                    ping_interval=self.heartbeat_interval,
                    ping_timeout=10, close_timeout=5,
                    open_timeout=10,
                )
                self._handle_open()

                # 接收循环
                async for message in self._ws:
                    parsed = None
                    if isinstance(message, str):
                        parsed = self._try_parse_json(message)
                    if parsed and self._on_event:
                        self._on_event(parsed)
                # 循环正常结束（可能是服务端主动关闭连接）时记录关闭信息
                try:
                    code = getattr(self._ws, "close_code", None)
                    reason = getattr(self._ws, "close_reason", None)
                    logger.warning(f"server closed connection code={code} reason={reason}")
                except Exception:
                    logger.warning("server closed connection (no code/reason)")
            except Exception as e:
                self._handle_error(e)

            finally:
                self._handle_close()
                self._ws = None

            if not self.reconnect or self._stop.is_set():
                break

    @staticmethod
    def _try_parse_json(s: Any) -> Optional[Dict[str, Any]]:
        # Accept dict directly
        if isinstance(s, dict):
            return s
        # Decode bytes
        if isinstance(s, (bytes, bytearray)):
            try:
                s = s.decode("utf-8", errors="ignore")
            except Exception:
                return None
        # Parse string JSON
        if isinstance(s, str):
            try:
                return json.loads(s)
            except Exception:
                return None
        return None

    # ---------- Internal handlers ----------
    def _handle_open(self) -> None:
        try:
            self._connected = True
            logger.info("connected")
            self.send_json({
                "method": "system", "action": "hello", 
                "detail": "hi, i am mcp-feishu-bot",
            })
        except Exception:
            pass

    def _handle_close(self) -> None:
        try:
            # 仅在确实建立过连接且当前连接已关闭时打印，并做节流
            if not self._ws:
                return
            is_closed = True
            try:
                is_closed = getattr(self._ws, "closed", True)
            except Exception:
                pass
            if self._connected and is_closed:
                now = time.time()
                if now - self._last_close_log_ts >= 2.0:
                    self._last_close_log_ts = now
                    code = getattr(self._ws, "close_code", None)
                    reason = getattr(self._ws, "close_reason", None)
                    logger.warning(f"disconnected code={code} reason={reason}")
            self._connected = False
        except Exception:
            pass

    def _handle_error(self, e: Exception) -> None:
        try:
            now = time.time()
            # 限制错误日志打印频率（每 2 秒最多一次）
            if now - self._last_error_log_ts >= 2.0:
                self._last_error_log_ts = now
                if isinstance(e, (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK)):
                    code = getattr(e, 'code', None)
                    reason = getattr(e, 'reason', None)
                    logger.warning(f"connection closed (exception) code={code} reason={reason}")
        except Exception:
            pass

    # ---------- Utilities ----------
    def _is_ws_connected(self) -> bool:
        """判断当前 WS 是否处于连接状态。"""
        ws = self._ws
        try:
            return ws is not None and not getattr(ws, "closed", False)
        except Exception:
            return False

    def _ensure_ws_connected(self) -> None:
        """在需要时触发快速重连；若线程未运行则启动。"""
        # 若已请求停止，不做重连
        if self._stop.is_set():
            return
        # 若后台线程未运行，直接启动
        if not self._thread or not self._thread.is_alive():
            try:
                logger.info("WS thread not running, restarting...")
                self.start()
                return
            except Exception:
                pass
        # 若 WS 未连接/已关闭，设置快速重连信号（带冷却）
        if not self._is_ws_connected():
            try:
                now = time.time()
                # 在冷却时间内不重复发送快速重连信号，避免频繁重试
                if (not self._reconnect_signal.is_set()) and (now - self._last_reconnect_ts >= self.RECONNECT_COOLDOWN):
                    logger.warning("WS lost, signaling fast reconnect...")
                    self._reconnect_signal.set()
                    self._last_reconnect_ts = now
            except Exception:
                pass
