#!/usr/bin/env python3
"""
Relay Handle

在 Agent 与 Feishu 之间进行事件归一化与记录：
- 从 Agent 的自定义消息事件映射为统一结构，仅记录/输出
- 从 Feishu 的消息/自定义事件归一化为统一结构，仅记录/输出
"""

from typing import Any, Dict, Optional

import lark_oapi as lark  # 仅用于类型提示与兼容
from lark_oapi.api.im.v1 import (
    EventMessage, EventSender,
    P2ImMessageReceiveV1Data
)


class RelayHandle:
    def __init__(self) -> None:
        # 仅做事件归一化与记录，不持有任何客户端
        pass
    
    def on_agent_event(self, payload: dict) -> None:
        """处理 Agent 事件，归一化并记录。"""
        # 当无法解析为 JSON 时，按普通消息处理
        if not isinstance(payload, dict):
            print(f"[Agent] unknown payload: {payload}")
            return

        method = payload.get("method")
        action = payload.get("action")
        detail = payload.get("detail")
        taskid = payload.get("taskid")
        if method != "message" :
            print(f"[Agent]  {method}, {action}, {detail}")
            return
        
        act_list = ["user-input", "stream", "change"]
        if action == "errors":
            self._on_errors(action, detail, taskid)
        elif action == "respond":
            self._on_respond(action, detail, taskid)
        elif action == "control":
            self._on_control(action, detail, taskid)
        elif action not in act_list:
            print(f"[Agent] unknown method: {method}, payload: {payload}")

    def on_feishu_msg(self, payload: P2ImMessageReceiveV1Data) -> None:
        """处理 Feishu 事件，归一化并记录。"""
        print(f'[Message Received] data: {lark.JSON.marshal(payload, indent=4)}')

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
        print(f"[Agent] errors {taskid}, {action}, {detail}")

    def _on_respond(self, action: Optional[str], detail: Any, taskid: Optional[str]) -> None:
        print(f"[Agent] respond {taskid}, {action}, {detail}")

    def _on_control(self, action: Optional[str], detail: Any, taskid: Optional[str]) -> None:
        print(f"[Agent] control {taskid}, {action}, {detail}")

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
        print(f'[Message Received] text: {msg.content}, sender: {lark.JSON.marshal(sender, indent=4)}')

    
    def _on_image_msg(self, msg: EventMessage, sender: EventSender) -> None:
        """
        Process image message events
        Override this method in subclasses to implement custom image message processing
        
        Args:
            message: Message object
            sender: Sender information
        """
        pass
    
    def _on_file_msg(self, msg: EventMessage, sender: EventSender) -> None:
        """
        Process file message events
        Override this method in subclasses to implement custom file message processing
        
        Args:
            message: Message object
            sender: Sender information
        """
        pass
   