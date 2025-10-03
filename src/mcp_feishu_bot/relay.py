#!/usr/bin/env python3
from cmath import e
"""
Relay Handle

在 Robot 与 Feishu 之间进行事件归一化与记录：
- 从 Robot 的自定义消息事件映射为统一结构，仅记录/输出
- 从 Feishu 的消息/自定义事件归一化为统一结构，仅记录/输出
"""

from typing import Any, Dict, Optional
import json

import lark_oapi as lark  # 仅用于类型提示与兼容
from lark_oapi.api.im.v1 import (
    EventMessage, EventSender,
    P2ImMessageReceiveV1Data
)

from .robot import RobotClient
from .msg import MsgHandle


class RelayHandle:
    def __init__(self) -> None:
        # 仅做事件归一化与记录，不持有任何客户端
        pass
    
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
            print(f"[Relay] unknown payload: {payload}")
            return

        method = payload.get("method")
        method_list = ["system", "message"]
        if method not in method_list:
            return
        
        taskid = payload.get("taskid")
        action = payload.get("action")
        detail = payload.get("detail")
        try:
            act_list = [
                "user-input", "hello",
                "stream", "change",
            ]
            if action == "errors":
                self._on_errors(action, detail, taskid)
            elif action == "respond":
                self._on_respond(action, detail, taskid)
            elif action == "control":
                self._on_control(action, detail, taskid)
            elif action == "welcome":
                print(f"[Relay] connect success: {detail}")
            elif action not in act_list:
                print(f"[Relay] unknown action: {action}, payload: {payload}")
        except Exception as e:
            print(f"[Relay] error: {e}, payload: {payload}")

    def on_feishu_msg(self, payload: P2ImMessageReceiveV1Data) -> None:
        """处理 Feishu 事件，归一化并记录。"""
        # print(f'[Message Received] data: {lark.JSON.marshal(payload, indent=4)}')

        # Extract message information
        message = payload.message
        sender = payload.sender
        msg_type = message.message_type
        if msg_type == "text":
            self._on_text_msg(message, sender)
        elif msg_type == "image":
            self._on_image_msg(message, sender)
        elif msg_type == "file":
            self._on_file_msg(message, sender)
            
    # ---------- Agent -> Relay ----------
    def _on_errors(self, action: Optional[str], detail: Any, taskid: Optional[str]) -> None:
        print(f"[Relay] errors {taskid}, {action}, {detail}")

    def _on_respond(self, action: Optional[str], detail: Dict[str, Any], taskid: Optional[str]) -> None:
        # Normalize detail to dict
        if not isinstance(detail, dict):
            print(f"[Relay] unknown detail: {detail}")
            return
        # Extract actions
        content_parts: list[str] = []
        actions = detail.get("actions") or []
        if not isinstance(actions, list):
            for item in actions:
                type = item.get("type")
                if type == 'tool-result':
                    txt = item.get("content")
                    content_parts.append(txt.strip())
                elif type == 'complete':
                    txt = item.get("content")
                    content_parts.append(txt.strip())
        else:
            content_parts.append(detail.get("thinking", "").strip())
        
        # send respond to feishu
        email="chnwine@qq.com"
        reply="\n\n".join(content_parts).strip()
        self.feishu.send_text(content=reply, receive_id=email)
        print(f"[Relay] respond task={taskid}, action={action}")

    def _on_control(self, action: Optional[str], detail: Any, taskid: Optional[str]) -> None:
        print(f"[Relay] control {taskid}, {action}, {detail}")

    # ---------- Feishu -> Relay ----------
    def _on_custom_event(self, data: lark.CustomizedEvent) -> None:
        """
        Handle custom events (v1.0)
        Override this method in subclasses to implement custom event handling
        
        Args:
            data: Custom event data
        """
        print(f'[Custom Event] type: {data.type}, data: {lark.JSON.marshal(data, indent=4)}')
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
            print(f'[Relay] text: {msg.content}, sender: {lark.JSON.marshal(sender, indent=4)}')
            return
        try:
            # 立即回复一个 OneSecond 表情
            self.feishu.reply_emoji(msg.message_id, emoji_type="OneSecond")
            # 解析 JSON 内容, 并转发给机器人
            data = json.loads(msg.content) or {'text': ""}
            resp = self.robot.send_msg(data['text'])
            if "errmsg" in resp:
                print(f"[Relay] error message: {msg.content}")
                return
            if 'message' in resp:
                self.feishu.reply_text(msg.message_id, resp['message'])
                print(f"[Relay] reply text: {msg.content}, resp: {resp}")
            if 'emoji' in resp:
                self.feishu.reply_emoji(msg.message_id, resp['emoji'])
                print(f"[Relay] reply emoji: {msg.content}, resp: {resp}")
        except Exception as e:
            print(f"[Relay] failed to reply text: {msg.content}, error: {e}")
    
    def _on_image_msg(self, msg: EventMessage, sender: EventSender) -> None:
        """
        Process image message events
        Override this method in subclasses to implement custom image message processing
        
        Args:
            message: Message object
            sender: Sender information
        """
        if self.robot is None:
            print(f'[Relay] image: {msg.content}, sender: {lark.JSON.marshal(sender, indent=4)}')
            return
        try:
            # 立即回复一个 OneSecond 表情
            self.feishu.reply_emoji(msg.message_id, emoji_type="OneSecond")
            data = json.loads(msg.content) or {'image_key': ""}
            resp = self.feishu.save_image(msg.message_id, data['image_key'])
            if resp.success():
                # todo 发送图片到机器人
                print(f"[Relay] reply image: {msg.content}, resp: {resp}")
        except Exception as e:
            print(f"[Relay] failed to reply image: {msg.content}, error: {e}")
    
    def _on_file_msg(self, msg: EventMessage, sender: EventSender) -> None:
        """
        Process file message events
        Override this method in subclasses to implement custom file message processing
        
        Args:
            message: Message object
            sender: Sender information
        """
        if self.robot is None:
            print(f'[Relay] file: {msg.content}, sender: {lark.JSON.marshal(sender, indent=4)}')
            return
        try:
            # 立即回复一个 OneSecond 表情
            self.feishu.reply_emoji(msg.message_id, emoji_type="OneSecond")
            data = json.loads(msg.content) or {'file_key': ""}
            resp = self.feishu.save_file(msg.message_id, data['file_key'])
            if resp.success():
                # todo 发送文件到机器人
                print(f"[Relay] reply file: {msg.content}, resp: {resp}")
        except Exception as e:
            print(f"[Relay] failed to reply file: {msg.content}, error: {e}")

