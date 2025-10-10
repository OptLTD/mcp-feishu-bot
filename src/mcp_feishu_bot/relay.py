#!/usr/bin/env python3
from cmath import e
"""
Relay Handle

在 Robot 与 Feishu 之间进行事件归一化与记录：
- 从 Robot 的自定义消息事件映射为统一结构，仅记录/输出
- 从 Feishu 的消息/自定义事件归一化为统一结构，仅记录/输出
"""

import json,time, threading
from typing import Any, Dict, Optional

import lark_oapi as lark  # 仅用于类型提示与兼容
from lark_oapi.api.im.v1 import (
    EventMessage, EventSender, UserId,
    P2ImMessageReceiveV1Data
)

from .robot import RobotClient
from .msg import MsgHandle
from fastmcp.utilities.logging import get_logger

logger = get_logger(__name__)


class RelayHandle:
    def __init__(self) -> None:
        # 仅做事件归一化与记录，不持有任何客户端
        pass
        # 事件去重：记录已处理的 trace_id 及其时间，用于过滤重复事件
        self._seen_trace_ids: Dict[str, float] = {}
        # 维护去重集合的 TTL，避免无限增长（默认 30 分钟）
        self._dedup_ttl_seconds: int = 1800

        # 会话级待处理上下文：按 chat_id 缓存文本与上传文件
        # 结构：{ 
        #   'chat_id': { 'text': str|None, 'uploads': list[str], 
        #   'timer': threading.Timer|None, 'message_id': str|None } 
        # }
        self._pending_intents: Dict[str, Dict[str, Any]] = {}
        # 缓存已知会话（chat_id 即 session key），用于简单标记/统计
        self._cached_sessions: Dict[str, bool] = {}

    def set_feishu(self, feishu: MsgHandle) -> None:
        """初始化 Relay 句柄，绑定 Feishu 客户端。"""
        self.feishu = feishu

    def set_robot(self, robot: RobotClient) -> None:
        """初始化 Relay 句柄，绑定 Robot 客户端。"""
        self.robot = robot


    def on_robot_event(self, payload: dict) -> None:
        """处理 Robot 事件，归一化并记录。"""
        # 当无法解析为 JSON 时，按普通消息处理
        if not isinstance(payload, dict):
            logger.warning(f"unknown payload: {payload}")
            return

        method = payload.get("method")
        method_list = ["system", "message"]
        if method not in method_list:
            return
        
        sessid = payload.get("sessid")
        action = payload.get("action")
        detail = payload.get("detail")
        if sessid not in self._cached_sessions:
            logger.warning(f"session err:{sessid}/{detail}")
            return
        try:
            act_list = [
                "user-input", "hello",
                "stream", "change",
            ]
            if action == "stream":
                pass
            elif action == "errors":
                self._on_errors(action, detail, sessid)
            elif action == "respond":
                self._on_respond(action, detail, sessid)
            elif action == "control":
                self._on_control(action, detail, sessid)
            elif action == "welcome":
                logger.info(f"connect success: {detail}")
            elif action not in act_list:
                logger.warning(f"unknown action: {action}, payload: {payload}")
            else:
                logger.warning(f"unknown method: {method}, payload: {payload}")
        except Exception as e:
            logger.error(f"error: {e}, payload: {payload}")

    def on_feishu_msg(self, payload: P2ImMessageReceiveV1Data) -> None:
        """处理 Feishu 事件，归一化并记录。"""
        # print(f'[Message Received] data: {lark.JSON.marshal(payload, indent=4)}')
     
        # 0) 定期清理过期的去重记录
        now_sec = int(time.time())
        self._prune_seen(now_sec)

        # 1) 按 trace_id 过滤重复事件
        trace_id = payload.message.message_id
        if trace_id in self._seen_trace_ids:
            logger.info(f"duplicate event ignored: trace_id={trace_id}")
            return

        # 2) 丢弃 10 分钟之前的消息
        msg_ts = int(payload.message.create_time) 
        if now_sec - (msg_ts // 1000) > 600:
            logger.info(f"expired message, msg_id={trace_id}")
            return
        
        # Extract message information
        sender = payload.sender
        message = payload.message
        msg_type = message.message_type
        chat_id = payload.message.chat_id
        if chat_id not in self._cached_sessions:
            self._cached_sessions[chat_id] = {
                'user_id':sender.sender_id,
                'create_time': message.create_time,
                'update_time': message.update_time,
                'is_group': message.chat_type == 'group',
            }
        else:
            # 更新会话的 update_time 为最新消息时间
            self._cached_sessions[chat_id].update({
                'update_time': message.update_time,
            })
        
        self._seen_trace_ids[trace_id] = now_sec
        if msg_type == "text":
            self._on_text_msg(message, sender)
        elif msg_type == "image":
            self._on_image_msg(message, sender)
        elif msg_type == "file":
            self._on_file_msg(message, sender)

    def _prune_seen(self, now_sec: int) -> None:
        """清理超过 TTL 的 trace_id 记录，避免集合无限增长。"""
        cutoff = now_sec - self._dedup_ttl_seconds
        stale_keys = [k for k, v in self._seen_trace_ids.items() if v < cutoff]
        for k in stale_keys:
            try:
                del self._seen_trace_ids[k]
            except KeyError:
                pass
            
    # ---------- Agent -> Relay ----------
    def _on_errors(self, action: Optional[str], detail: Any, session: Optional[str]) -> None:
        logger.error(f"errors {session}, {action}, {detail}")

    def _on_respond(self, action: Optional[str], detail: Dict[str, Any], sessid: Optional[str]) -> None:
        # Normalize detail to dict
        if not isinstance(detail, dict):
            logger.warning(f"unknown detail: {detail}")
            return

        # send respond to feishu
        card_head = {
            "title": "", "tags": "",
        }

        has_tool_result = False
        tool_result: list[str] = []
        for item in detail.get("actions") or []:
            type = item.get("type")
            if type == 'make-ask':
                has_tool_result = True
                txt = item.get("question")
                tool_result.append(txt.strip())
                opts = item.get("options") or []
                tool_result.append("\n".join(opts))
                card_head['title'] = '寻求帮助'
                card_head['tags'] = 'HELP'
                continue
            if type == 'complete':
                has_tool_result = True
                txt = item.get("content")
                tool_result.append(txt.strip())
                card_head['title'] = '任务完成'
                card_head['tags'] = 'DONE'
                continue
        if not has_tool_result:
            logger.info(f"not finish: {detail}")
            return
        
        card_detail = {
            "head": card_head,
            "body": "\n\n".join(tool_result).strip(),
        }
        session = self._cached_sessions[sessid]
        if session and session['user_id']:
            user: UserId = session['user_id']
            self.feishu.send_card(
                content=card_detail, 
                receive_id=user.open_id, 
                receive_id_type="open_id",
            )
            logger.info(f"respond task={sessid}, action={action}, open_id={user.open_id}")
        else:
            logger.warning(f"respond task={sessid}, action={action}, no user_id")

    def _on_control(self, action: Optional[str], detail: Any, sessid: Optional[str]) -> None:
        logger.info(f"control {sessid}, {action}, {detail}")

    # ---------- Feishu -> Relay ----------
    def _on_custom_event(self, data: lark.CustomizedEvent) -> None:
        """
        Handle custom events (v1.0)
        Override this method in subclasses to implement custom event handling
        
        Args:
            data: Custom event data
        """
        logger.info(f"[Custom Event] type: {data.type}, data: {lark.JSON.marshal(data, indent=4)}")
        # Normalize and emit via callback
        try:
            normalized = {
                "source": "feishu",
                "type": "custom_event",
                "event_type": getattr(data, "type", None),
                "raw": lark.JSON.marshal(data) if hasattr(lark, "JSON") else str(data),
            }
            self._emit_event("feishu.custom", normalized)
        except Exception:
            self._emit_event("feishu.custom", {"error": "normalize_failed"})

    def _on_text_msg(self, msg: EventMessage, sender: EventSender) -> None:
        """
        Process text message events
        Override this method in subclasses to implement custom text message processing
        
        Args:
            message: Message object
            sender: Sender information
        """
        if self.robot is None:
            logger.info(f"text: {msg.content}, sender: {lark.JSON.marshal(sender, indent=4)}")
            return
        try:
            # 立即回复一个 OneSecond 表情
            self.feishu.reply_emoji(msg.message_id, "OneSecond")
            data = json.loads(msg.content) or {'text': ""}

            # 如果之前缓存了文件（先图片后文字），则合并为一次完整调用
            state = self._pending_intents.get(msg.chat_id) or {}
            if (state.get('uploads') or []) and not state.get('text'):
                # 合并上下文调用机器人 并清理状态
                uploads = list(state.get('uploads') or [])
                # 传递 chat_id 作为会话标识，避免多用户串话
                intent = self.robot.get_intent(
                    content=data['text'], 
                    uploads=uploads, 
                    session=msg.chat_id
                )
                if state.get('timer'):
                    state.get('timer').cancel()
                self._pending_intents.pop(msg.chat_id, None)
            else:
                # 直接意图识别
                intent = self.robot.get_intent(
                    content=data['text'], 
                    session=msg.chat_id
                )

            if "errmsg" in intent and intent['errmsg']:
                logger.error(f"error message: {intent['errmsg']}")
                self.feishu.reply_text(msg.message_id, intent['errmsg'])
                return
            if 'message' in intent and intent['message']:
                self.feishu.reply_text(msg.message_id, intent['message'])
                logger.info(f"reply text: {msg.content}, resp: {intent}")
            if 'emoji' in intent and intent['emoji']:
                self.feishu.reply_emoji(msg.message_id, intent['emoji'])
                logger.info(f"reply emoji: {msg.content}, resp: {intent}")

            # send intent to robot if intent == 'wait'
            is_type_wait = 'intent' in intent and intent['intent'] == 'wait'
            is_msg_wait = 'message' in intent and intent['message'] == 'wait'
            if is_type_wait or is_msg_wait:
                self._cache_intent(msg, 10)
        except Exception as e:
            logger.error(f"failed to reply text: {msg.content}, error: {e}")
    
    def _on_image_msg(self, msg: EventMessage, sender: EventSender) -> None:
        """
        Process image message events
        Override this method in subclasses to implement custom image message processing
        
        Args:
            message: Message object
            sender: Sender information
        """
        if self.robot is None:
            logger.info(f"image: {msg.content}, sender: {lark.JSON.marshal(sender, indent=4)}")
            return
        try:
            # 立即回复一个 OneSecond 表情
            # self.feishu.reply_emoji(msg.message_id, emoji_type="OneSecond")
            data = json.loads(msg.content) or {'image_key': ""}
            saved = self.feishu.save_image(msg.message_id, data['image_key'])
            if saved.success():
                self._cache_upload(msg, saved.file_name)
        except Exception as e:
            logger.error(f"failed to reply image: {msg.content}, error: {e}")
    
    def _on_file_msg(self, msg: EventMessage, sender: EventSender) -> None:
        """
        Process file message events
        Override this method in subclasses to implement custom file message processing
        
        Args:
            message: Message object
            sender: Sender information
        """
        if self.robot is None:
            logger.info(f"file: {msg.content}, sender: {lark.JSON.marshal(sender, indent=4)}")
            return
        try:
            # 立即回复一个 OneSecond 表情
            # self.feishu.reply_emoji(msg.message_id, emoji_type="OneSecond")
            data = json.loads(msg.content) or {'file_key': ""}
            saved = self.feishu.save_file(msg.message_id, data['file_key'])
            if saved.success():
                self._cache_upload(msg, saved.file_name)
        except Exception as e:
            logger.error(f"failed to reply file: {msg.content}, error: {e}")

    def _cache_intent(self, msg: EventMessage, timeout: int = 10) -> None:
        """缓存待处理意图，等待后续文件/图片合并调用机器人。"""
        data = json.loads(msg.content) or {'text': ""}
        state = self._pending_intents.get(msg.chat_id) or {
            'text': None, 'uploads': [],
            'timer': None, 'message_id': None,
        }
        self._pending_intents[msg.chat_id] = {
            'text': data.get('text', ''), 'timer': None,
            'uploads': state.get('uploads') or [],
            'message_id': msg.message_id,
        }
        self._set_timer(msg, timeout)


    def _cache_upload(self, msg: EventMessage, filename: str) -> None:
        """缓存上传文件，并在存在待处理文字时与文字合并调用机器人。"""
        chat_id, msg_id = msg.chat_id, msg.message_id
        logger.info(f"cached upload for chat={chat_id}: {filename}")
        state = self._pending_intents.get(chat_id) or {
            'text': None, 'uploads': [],
            'timer': None, 'message_id': None,
        }
        # 写回状态；仅在已有文字等待时才启动定时器
        state['uploads'].append(filename)
        if state.get('text'):
            self._set_timer(msg, timeout = 10)
        self._pending_intents[chat_id] = state

        if not state.get('text'):
            logger.info(f"really cached upload for chat={chat_id}: {filename}")
            return
        
        try:
            # 上传与文字合并时也传递会话标识
            resp = self.robot.get_intent(
                content=state['text'], 
                uploads=state['uploads'], 
                session=chat_id
            )
            if 'message' in resp and resp['message']:
                self.feishu.reply_text(msg_id, resp['message'])
            if 'emoji' in resp and resp['emoji']:
                self.feishu.reply_emoji(msg_id, resp['emoji'])
            logger.info(f"merged text+uploads: {resp}")
        except Exception as ex:
            logger.error(f"failed to merge text+uploads: {ex}")
        finally:
            if state.get('timer'):
                state['timer'].cancel()
            self._pending_intents.pop(chat_id, None)
    
    # 设置超时定时器
    def _set_timer(self, msg: EventMessage, timeout: int = 10):
        # 先取消已存在定时器，然后重新设置新定时器
        chat_id = msg.chat_id
        state = self._pending_intents.get(chat_id)
        if not state:
            state = {
                'text': None,
                'uploads': [],
                'timer': None,
                'message_id': msg.message_id,
            }
            self._pending_intents[chat_id] = state
        else:
            # 确保 message_id 记录有值
            state['message_id'] = state.get('message_id') or msg.message_id

        if state.get('timer'):
            state['timer'].cancel()
        timer = threading.Timer(timeout, self._on_timeout, args=(chat_id,))
        state['timer'] = timer
        timer.start()

    # 启动10秒超时：若10秒未收到新文件，则结束等待并按纯文本处理
    def _on_timeout(self, chat_id: str):
        if not self._pending_intents.get(chat_id):
            return
        state = self._pending_intents.get(chat_id)
        text = state.get('text') or ''
        uploads = state.get('uploads') or []
        if not text or len(uploads) == 0:
            # 没有文字则直接清理，不进行空意图请求
            self._pending_intents.pop(chat_id, None)
            logger.info(f"wait-timeout abort: {chat_id}")
            return
        # 如果仍未有附件，触发一次仅文本的处理并清理状态
        try:
            # 纯文本处理同样携带会话，避免上下文串话
            resp = self.robot.get_intent(text, session=chat_id)
            if 'message' in resp:
                msg_id = state.get('message_id')
                self.feishu.reply_text(msg_id, resp['message'])
            if 'emoji' in resp:
                msg_id = state.get('message_id')
                self.feishu.reply_emoji(msg_id, resp['emoji'])
            logger.info(f"wait-timeout proceed text-only: {resp}")
        except Exception as ex:
            logger.error(f"wait-timeout failed: {ex}")
        finally:
            self._pending_intents.pop(chat_id, None)
