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
    CreateFileRequest, CreateFileRequestBody
)

from mcp_feishu_bot.client import FeishuClient


class MessageHandle(FeishuClient):
    """
    Feishu message client with comprehensive messaging functionality
    """
    
    def send_text(self, receive_id: str, content: str, 
            msg_type: str = "text", receive_id_type: str = "email"
        ) -> Dict[str, Any]:
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
        try:
            # If content is a string, convert to JSON string
            if isinstance(content, str):
                # Try to parse as JSON, if it fails, wrap as text message
                try:
                    json.loads(content)
                    # If successful, content is already valid JSON
                except (json.JSONDecodeError, ValueError):
                    # If parsing fails, wrap the content as a text message
                    content = json.dumps({
                        'text': content
                    }, ensure_ascii=False)
            else:
                content = json.dumps(content, ensure_ascii=False)

            # Create message request
            request = CreateMessageRequest.builder() \
                .receive_id_type(receive_id_type) \
                .request_body(CreateMessageRequestBody.builder()
                              .receive_id(receive_id)
                              .msg_type(msg_type)
                              .content(content)
                              .build()) \
                .build()
            # Send message
            response = self.http_client.im.v1.message.create(request)
            
            if response.success():
                return {
                    "success": True,
                    "message_id": response.data.message_id,
                    "create_time": response.data.create_time
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to send message: {response.msg}",
                    "code": response.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }
    
    def send_file(self, receive_id: str, file_path: str, 
                 receive_id_type: str = "email", file_type: str = "stream") -> Dict[str, Any]:
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
        try:
            # Check if file exists
            if not os.path.exists(file_path):
                return {
                    "success": False,
                    "error": f"File not found: {file_path}"
                }
            
            file_name = os.path.basename(file_path)
            
            # Upload file first
            with open(file_path, 'rb') as file:
                create_file_req = CreateFileRequest.builder() \
                    .request_body( 
                      CreateFileRequestBody.builder()
                        .file_type(file_type)
                        .file_name(file_name)
                        .file(file).build()
                    ).build()
                
                create_file_resp = self.http_client.im.v1.file.create(create_file_req)
                
                if not create_file_resp.success():
                    return {
                        "success": False,
                        "error": f"Failed to upload file: {create_file_resp.msg}",
                        "code": create_file_resp.code
                    }
            
            # Send message with uploaded file
            option = lark.RequestOption.builder().headers({"X-Tt-Logid": create_file_resp.get_log_id()}).build()
            create_message_req = CreateMessageRequest.builder() \
                .receive_id_type(receive_id_type) \
                .request_body(CreateMessageRequestBody.builder()
                              .receive_id(receive_id)
                              .msg_type("file")
                              .content(lark.JSON.marshal(create_file_resp.data))
                              .build()) \
                .build()
            
            create_message_resp = self.http_client.im.v1.message.create(create_message_req, option)
            
            if create_message_resp.success():
                return {
                    "success": True,
                    "message_id": create_message_resp.data.message_id,
                    "file_key": create_file_resp.data.file_key
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to send file message: {create_message_resp.msg}",
                    "code": create_message_resp.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }

    def send_text_markdown(self, receive_id: str, content: str,
                            msg_type: str = "text", receive_id_type: str = "email") -> str:
        """Send text and return a Markdown summary.
        Intention: Move formatting out of main into handle for reuse.
        """
        resp = self.send_text(receive_id, content, msg_type, receive_id_type)
        if not resp.get("success"):
            error_title = resp.get("error", "Failed to send text")
            code = resp.get("code")
            details = [f"receive_id: {receive_id}", f"receive_id_type: {receive_id_type}", f"msg_type: {msg_type}"]
            if code is not None:
                details.append(f"code: {code}")
            return f"# error: {error_title}\n" + "\n".join(details)
        lines = ["---", "# Message Sent", f"receive_id: {receive_id}", f"receive_id_type: {receive_id_type}", f"msg_type: {msg_type}"]
        msg_id = resp.get("message_id") or resp.get("data", {}).get("message_id")
        if msg_id:
            lines.append(f"message_id: {msg_id}")
        return "\n".join(lines)

    def send_image_markdown(self, receive_id: str, image_path: str, receive_id_type: str = "email") -> str:
        """Send image and return a Markdown summary.
        Intention: Move formatting out of main into handle for reuse.
        """
        resp = self.send_image(receive_id, image_path, receive_id_type)
        if not resp.get("success"):
            error_title = resp.get("error", "Failed to send image")
            code = resp.get("code")
            details = [f"receive_id: {receive_id}", f"receive_id_type: {receive_id_type}", f"image_path: {image_path}"]
            if code is not None:
                details.append(f"code: {code}")
            return f"# error: {error_title}\n" + "\n".join(details)
        lines = ["---", "# Image Sent", f"receive_id: {receive_id}", f"receive_id_type: {receive_id_type}", f"image_path: {image_path}"]
        image_key = resp.get("image_key") or resp.get("data", {}).get("image_key")
        if image_key:
            lines.append(f"image_key: {image_key}")
        return "\n".join(lines)

    def send_file_markdown(self, receive_id: str, file_path: str,
                            receive_id_type: str = "email", file_type: str = "stream") -> str:
        """Send file and return a Markdown summary.
        Intention: Move formatting out of main into handle for reuse.
        """
        resp = self.send_file(receive_id, file_path, receive_id_type, file_type)
        if not resp.get("success"):
            error_title = resp.get("error", "Failed to send file")
            code = resp.get("code")
            details = [f"receive_id: {receive_id}", f"receive_id_type: {receive_id_type}", f"file_path: {file_path}", f"file_type: {file_type}"]
            if code is not None:
                details.append(f"code: {code}")
            return f"# error: {error_title}\n" + "\n".join(details)
        lines = ["---", "# File Sent", f"receive_id: {receive_id}", f"receive_id_type: {receive_id_type}", f"file_path: {file_path}", f"file_type: {file_type}"]
        file_token = resp.get("file_token") or resp.get("data", {}).get("file_token")
        if file_token:
            lines.append(f"file_token: {file_token}")
        return "\n".join(lines)

    def send_image(self, receive_id: str, image_path: str, 
                  receive_id_type: str = "email") -> Dict[str, Any]:
        """
        Send an image to a Feishu user or group
        
        Args:
            receive_id: The ID of the message receiver
            image_path: Path to the image file to send
            receive_id_type: Type of receiver ID
            
        Returns:
            Dictionary containing the result of the image sending operation
        """
        try:
            # Check if image file exists
            if not os.path.exists(image_path):
                return {
                    "success": False,
                    "error": f"Image file not found: {image_path}"
                }
            
            # Upload image first
            with open(image_path, 'rb') as image_file:
                create_image_req = CreateImageRequest.builder() \
                    .request_body(CreateImageRequestBody.builder()
                                  .image_type("message")
                                  .image(image_file)
                                  .build()) \
                    .build()
                
                create_image_resp = self.http_client.im.v1.image.create(create_image_req)
                
                if not create_image_resp.success():
                    return {
                        "success": False,
                        "error": f"Failed to upload image: {create_image_resp.msg}",
                        "code": create_image_resp.code
                    }
            
            # Send message with uploaded image
            option = lark.RequestOption.builder().headers({"X-Tt-Logid": create_image_resp.get_log_id()}).build()
            create_message_req = CreateMessageRequest.builder() \
                .receive_id_type(receive_id_type) \
                .request_body(CreateMessageRequestBody.builder()
                              .receive_id(receive_id)
                              .msg_type("image")
                              .content(lark.JSON.marshal(create_image_resp.data))
                              .build()) \
                .build()
            
            create_message_resp = self.http_client.im.v1.message.create(create_message_req, option)
            
            if create_message_resp.success():
                return {
                    "success": True,
                    "message_id": create_message_resp.data.message_id,
                    "image_key": create_image_resp.data.image_key
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to send image message: {create_message_resp.msg}",
                    "code": create_message_resp.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }
