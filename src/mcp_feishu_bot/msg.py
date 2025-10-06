#!/usr/bin/env python3
"""
Feishu Message Operations

Message-related operations for Feishu (Lark) API including:
- Text message sending
- Image message sending
- File message sending
"""

import json, os
import warnings, logging
from typing import Dict, Any

# Suppress deprecation warnings from lark_oapi library
warnings.filterwarnings("ignore", category=DeprecationWarning)

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateMessageRequest, CreateMessageRequestBody,
    CreateImageRequest, CreateImageRequestBody,
    CreateFileRequest, CreateFileRequestBody,
    CreateMessageResponse, Emoji,
    CreateMessageReactionRequest, 
    CreateMessageReactionResponse,
    CreateMessageReactionRequestBody, 
    ReplyMessageRequestBody, 
    ReplyMessageRequest,
    ReplyMessageResponse,
    GetMessageResourceRequest, 
    GetMessageResourceResponse,
)

from mcp_feishu_bot.client import FeishuClient
from fastmcp.utilities.logging import get_logger

logger = get_logger(__name__)

class MsgHandle(FeishuClient):
    """
    Feishu message client with comprehensive messaging functionality
    """
    
    def send_text(self, receive_id: str, content: str, 
            msg_type: str = "text", receive_id_type: str = "email"
        ) -> CreateMessageResponse:
        """
        Send a message to a Feishu user or group
        
        Args:
            receive_id: The ID of the message receiver
            content: The message content
            msg_type: Message type (text, rich_text, etc.)
            receive_id_type: Type of receiver ID (email, open_id, user_id, union_id, chat_id)
            
        Returns:
            Dictionary containing the result of the message sending operation
        """
        if isinstance(content, str):
            try:
                json.loads(content)
            except (json.JSONDecodeError, ValueError):
                meta= { 'text': content }
                content = json.dumps(meta)
        else:
            content = json.dumps(content)

        # build payload
        logger.info(f"[MSG] send text: {content}")
        body = CreateMessageRequestBody.builder() \
                .content(content).msg_type(msg_type) \
                .receive_id(receive_id).build()
        request = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(body).build()
        return self.http_client.im.v1.message.create(request)
    
    def send_file(self, receive_id: str, file_path: str, 
                 receive_id_type: str = "email", file_type: str = "stream") -> CreateMessageResponse:
        """
        Send a file to a Feishu user or group
        
        Args:
            receive_id: The ID of the message receiver
            file_path: Path to the file to send
            receive_id_type: Type of receiver ID
            file_type: Type of file (stream, opus, mp4, pdf, doc, xls, ppt, etc.)
            
        Returns:
            Dictionary containing the result of the file sending operation
        """
        # Check if file exists
        storage_path = os.environ.get('STORAGE_PATH')
        os.chdir(storage_path) if storage_path else None
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        file_name = os.path.basename(file_path)
        file_body = CreateFileRequestBody.builder() \
            .file_type(file_type).file_name(file_name) \
            .file(open(file_path, 'rb')).build()
        file_req = CreateFileRequest.builder() \
            .request_body(file_body).build()
        file_res = self.http_client.im.v1.file.create(file_req)
        if not file_res.success():
            raise Exception(f"Failed to upload file: {file_res.msg}")

        # Send message with uploaded file
        msg_opt = lark.RequestOption.builder().headers({
            "X-Tt-Logid": file_res.get_log_id()
        }).build()
        msg_body = CreateMessageRequestBody.builder() \
            .content(lark.JSON.marshal(file_res.data)) \
            .receive_id(receive_id).msg_type("file").build()
        msg_req = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(msg_body).build()
        
        return self.http_client.im.v1.message.create(msg_req, msg_opt)


    def send_image(self, receive_id: str, image_path: str, 
                  receive_id_type: str = "email") -> CreateMessageResponse:
        """
        Send an image to a Feishu user or group
        
        Args:
            receive_id: The ID of the message receiver
            image_path: Path to the image file to send
            receive_id_type: Type of receiver ID
            
        Returns:
            Dictionary containing the result of the image sending operation
        """
        # Check if file exists
        storage_path = os.environ.get('STORAGE_PATH')
        os.chdir(storage_path) if storage_path else None
        if not os.path.exists(image_path):
            raise FileNotFoundError(f"Image file not found: {image_path}")
        
        # Upload image first
        image_body = CreateImageRequestBody.builder() \
                .image(open(image_path, 'rb'))\
                .image_type("message").build()
        image_req = CreateImageRequest.builder() \
            .request_body(image_body).build()
        image_resp = self.http_client.im.v1.image.create(image_req)
        if not image_resp.success():
            raise Exception(f"Failed to upload image: {image_resp.msg}")
        
        # Send message with uploaded image
        msg_opt = lark.RequestOption.builder().headers({
            "X-Tt-Logid": image_resp.get_log_id()
        }).build()
        image_body = CreateMessageRequestBody.builder() \
                .content(lark.JSON.marshal(image_resp.data)) \
                .receive_id(receive_id).msg_type("image").build()
        msg_req = CreateMessageRequest.builder() \
            .receive_id_type(receive_id_type) \
            .request_body(image_body).build()
        return self.http_client.im.v1.message.create(msg_req, msg_opt)

    def send_card(self, receive_id: str, content: dict, 
                  receive_id_type: str = "email") -> CreateMessageResponse:
        """
        Send an interactive card message.

        Args:
            receive_id: The ID of the message receiver (user_id/open_id/union_id/email/chat_id)
            content: Card content as dict or JSON string (must conform to schema 2.0)
            receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)

        Returns:
            CreateMessageResponse
        """
        # Ensure content is a valid JSON string for interactive card
        if not isinstance(content, dict):
            raise ValueError("content must be a valid JSON string for interactive card")

        # send text
        content_str = json.dumps(self._build_card(content), ensure_ascii=False)
        return self.send_text(receive_id, content_str, "interactive", receive_id_type)

    def reply_text(self, message_id: str, content: str, msg_type: str = "text") -> ReplyMessageResponse:
        """
        Reply a text message.

        Args:
            message_id: The target message ID to reply to
            content: Text content to send

        Returns:
            CreateMessageResponse from SDK
        """
        if not message_id:
            raise ValueError("message_id is required")
        if not content:
            raise ValueError("content is required")

        if isinstance(content, str):
            try:
                json.loads(content)
            except (json.JSONDecodeError, ValueError):
                meta= { 'text': content }
                content = json.dumps(meta, ensure_ascii=False)
        else:
            content = json.dumps(content, ensure_ascii=False)
        
        body = ReplyMessageRequestBody.builder() \
            .content(content).msg_type(msg_type).build()
        request = ReplyMessageRequest.builder() \
            .request_body(body).message_id(message_id).build()
        return self.http_client.im.v1.message.reply(request)

    def reply_emoji(self, message_id: str, emoji_type: str) -> CreateMessageReactionResponse:
        """
        Add an emoji reaction to a specific message.

        Args:
            message_id: The target message ID to react to
            emoji_type: Emoji type key (per Feishu docs)

        Returns:
            CreateMessageReactionResponse from SDK
        """
        if not message_id:
            raise ValueError("message_id is required")
        if not emoji_type:
            raise ValueError("emoji_type is required")

        emoji = Emoji.builder().emoji_type(emoji_type).build()
        body = CreateMessageReactionRequestBody.builder() \
            .reaction_type(emoji).build()
        request = CreateMessageReactionRequest.builder() \
            .message_id(message_id) \
            .request_body(body).build()
        return self.http_client.im.v1.message_reaction.create(request)

    def save_file(self, message_id: str, file_key: str, message_type: str = "file") -> GetMessageResourceResponse:
        """
        Save file to a local file.

        Args:
            message_id: The target message ID to save
            file_key: The file key of the file to save
        """
        request = GetMessageResourceRequest.builder() \
            .message_id(message_id).type(message_type) \
            .file_key(file_key).build()

        response = self.http_client.im.v1.message_resource.get(request)
        if response.success():
            storage_path = os.environ.get('STORAGE_PATH')
            os.chdir(storage_path) if storage_path else None
            if not response.file_name:
                response.file_name = f"file-{message_id}.raw"
            elif message_type == "image":
                response.file_name = f"image-{message_id}.png"
            f = open(f"{response.file_name}", "wb")
            f.write(response.file.read())
            f.close()
        return response

    def save_image(self, message_id: str,  file_key: str = "") -> GetMessageResourceResponse:
        """
        Save image to a file.

        Args:
            message_id: The target message ID to save
            file_key: The file key of the image to save
        """
        return self.save_file(message_id, file_key, "image")

    def _build_card(self, data: dict) -> str:
        """
        Build an interactive card message.
        The card content must conform to schema 2.0.
        card template: {
            head: {
                "tags": "DONE",
                "color": "blue",
                "title": "MCP 助手"
            },
            body: '## Hi, nice to meet you!',
            foot: {
                "text": "Click Me",
                "link": "https://www.feishu.cn/"
            }
        }

        Args:
            content: Card content as dict or JSON string (must conform to schema 2.0)

        Returns:
            JSON string of the interactive card message
        """
      
        result = {
            "schema": "2.0", 
            "config": {
                "update_multi": True 
            },
            "card_link": {},
            "header": {},
            "body": {
                "elements": []
            },
            "fallback": {}
        }

        # start build header
        head = data.get('head', {})
        if 'color' not in head:
            head['color'] = "blue"
        if 'title' in head:
            result['header']['title'] = {
                "tag": "plain_text",
                "content": head['title']
            }
            # 标题主题样式颜色。支持如下颜色，默认值 default：
            # "blue"|"wathet"|"turquoise"|"green"|"yellow"|
            # "orange"|"red"|"carmine"|"violet"|"purple"|
            # "indigo"|"grey"|"default"
            result['header']['template'] = head['color']
        if 'subtitle' in head:
            result['header']['subtitle'] = {
                "tag": "plain_text",
                "content": head['subtitle']
            }
        if "tags" in head:
            tags_list = []
            for text in head['tags'].split(','):
                tags_list.append({
                    "tag": "text_tag",
                    "color": head['color'],
                    "text": { # 标签内容
                        "tag": "plain_text",
                        "content": text
                    },
                })
            result['header']['text_tag_list'] = tags_list
        
        # start build body
        elements = []
        if 'body' in data:
            elements.append({
                "tag": "markdown",
                "content": data['body'],
                "text_align": "left",
                "text_size": "normal_v2",
                "margin": "0px 0px 0px 0px"
            })
        # start build footer
        foot = data.get('foot', {})
        if 'text' in foot:
            elements.append({
                "tag": "button",
                "text": {
                    "tag": "plain_text",
                    "content": foot['text']
                },
                "type": "default",
                "width": "default",
                "size": "medium",
                "behaviors": [
                    {
                        "type": "open_url",
                        "default_url": foot['link'],
                    }
                ],
                "margin": "0px 0px 0px 0px"
            })
        result['body']['elements'] = elements
        # finish all elements
        return result
