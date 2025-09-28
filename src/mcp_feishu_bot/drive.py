#!/usr/bin/env python3
"""
Feishu Drive Operations

Drive-related operations for Feishu (Lark) API including:
- List files in folders
- Delete files and folders
"""

import warnings
from typing import Dict, Any, Optional

# Suppress deprecation warnings from lark_oapi library
warnings.filterwarnings("ignore", category=DeprecationWarning)

import lark_oapi as lark
from lark_oapi.api.drive.v1 import (
    ListFileRequest,
    DeleteFileRequest
)

from mcp_feishu_bot.client import FeishuClient


class DriveHandle(FeishuClient):
    """
    Feishu Drive client with file and folder management functionality
    """
    
    def list_files(self, folder_token: str = "", page_size: int = 100, 
                         page_token: str = "", order_by: str = "EditedTime",
                         direction: str = "DESC", user_id_type: str = "email") -> Dict[str, Any]:
        """
        Get file list in a specified folder
        
        Args:
            folder_token: Token of the folder to list files from (empty for root directory)
            page_size: Number of items per page (default: 100, max: 200)
            page_token: Pagination token for next page
            order_by: Sort order (EditedTime or CreatedTime)
            direction: Sort direction (ASC or DESC)
            user_id_type: Type of user ID (open_id, union_id, user_id)
            
        Returns:
            Dictionary containing the file list and pagination info
        """
        try:
            # Build request
            request = ListFileRequest.builder() \
                .page_size(page_size) \
                .user_id_type(user_id_type) \
                .build()
            
            # Set optional parameters
            if page_token:
                request.page_token = page_token
            if folder_token:
                request.folder_token = folder_token
            if order_by:
                request.order_by = order_by
            if direction:
                request.direction = direction
            
            # Make API call
            response = self.http_client.drive.v1.file.list(request)
            
            if not response.success():
                return {
                    "success": False,
                    "error": {
                        "code": response.code, "msg": response.msg,
                        "request_id": getattr(response, 'request_id', '')
                    }
                }
            
            # Convert File objects to serializable dictionaries
            files_data = []
            files = getattr(response.data, 'files', [])
            if files:
                for file in files:
                    file_dict = {
                        "token": getattr(file, 'token', ''),
                        "name": getattr(file, 'name', ''),
                        "type": getattr(file, 'type', ''),
                        "parent_token": getattr(file, 'parent_token', ''),
                        "url": getattr(file, 'url', ''),
                        "size": getattr(file, 'size', 0),
                        "created_time": getattr(file, 'created_time', ''),
                        "modified_time": getattr(file, 'modified_time', ''),
                        "owner_id": getattr(file, 'owner_id', '')
                    }
                    files_data.append(file_dict)
            
            return {
                "success": True,
                "data": {
                    "files": files_data,
                    "has_more": getattr(response.data, 'has_more', False),
                    "page_token": getattr(response.data, 'page_token', ""),
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": {
                    "code": -1,
                    "msg": f"Exception occurred: {str(e)}",
                    "request_id": ""
                }
            }
    
    def delete_file(self, file_token: str, file_type: str) -> Dict[str, Any]:
        """
        Delete a file or folder
        
        Args:
            file_token: Token of the file or folder to delete
            file_type: Type of the file (file, docx, bitable, folder, doc)
            
        Returns:
            Dictionary containing the deletion result
        """
        try:
            # Build request
            request = DeleteFileRequest.builder() \
                .file_token(file_token) \
                .type(file_type) \
                .build()
            
            # Make API call
            response = self.http_client.drive.v1.file.delete(request)
            
            if not response.success():
                return {
                    "success": False,
                    "error": {
                        "code": response.code,
                        "msg": response.msg,
                        "request_id": response.get_request_id()
                    }
                }
            
            return {
                "success": True,
                "data": {
                    "task_id": response.data.task_id if hasattr(response.data, 'task_id') and response.data.task_id else None
                }
            }
            
        except Exception as e:
            return {
                "success": False,
                "error": {
                    "code": -1,
                    "msg": f"Exception occurred: {str(e)}",
                    "request_id": ""
                }
            }

    def describe_files_markdown(self, folder_token: str = "", options: Dict[str, Any] | None = None) -> str:
        """List files and return a Markdown summary with a JSON block.
        Intention: Move pagination and formatting out of main into the handle for reuse.
        """
        opts = options or {}
        page_size = int(opts.get("page_size", 50))
        order_by = str(opts.get("order_by", "EditedTime"))
        direction = str(opts.get("direction", "DESC"))
        user_id_type = str(opts.get("user_id_type", "email"))
        query = str(opts.get("query", ""))

        files: List[Dict[str, Any]] = []
        page_token = ""
        while True:
            resp = self.list_files(
                folder_token=folder_token,
                page_token=page_token,
                page_size=page_size,
                order_by=order_by,
                direction=direction,
                user_id_type=user_id_type,
            )
            if not resp.get("success"):
                err = resp.get("error") or {}
                code = err.get("code")
                msg = err.get("msg") or "Failed to list files"
                details = [f"folder_token: {folder_token}"]
                if query:
                    details.append(f"query: {query}")
                if code is not None:
                    details.append(f"code: {code}")
                return f"# error: {msg}\n" + "\n".join(details)

            data = resp.get("data") or {}
            batch = data.get("files") or []
            files.extend(batch)

            has_more = data.get("has_more")
            next_token = data.get("page_token")
            if not has_more or not next_token or len(files) >= page_size:
                break
            page_token = next_token

        # Format as Markdown with JSON payload
        lines: List[str] = []
        lines.append(f"# Drive files of ({len(files)}/{page_size})")
        payload = []
        for f in files[:page_size]:
            payload.append({
                "name": f.get("name"),
                "type": f.get("type"),
                "token": f.get("token"),
                "parent_token": f.get("parent_token"),
                "url": f.get("url")
            })
        import json as _json
        try:
            body = _json.dumps(payload, ensure_ascii=False, indent=2)
        except Exception:
            body = str(payload)
        lines.append("```json")
        lines.append(body)
        lines.append("```")
        return "\n".join(lines)

    def delete_file_markdown(self, file_token: str, file_type: str) -> str:
        """Delete a file and return a Markdown summary.
        Intention: Move formatting out of main into the handle for reuse.
        """
        resp = self.delete_file(file_token, file_type)
        if not resp.get("success"):
            err = resp.get("error") or {}
            code = err.get("code")
            msg = err.get("msg") or "Failed to delete file"
            details = [f"file_token: {file_token}", f"file_type: {file_type}"]
            if code is not None:
                details.append(f"code: {code}")
            return f"# error: {msg}\n" + "\n".join(details)
        lines = ["---", "# File Deleted", f"file_token: {file_token}", f"file_type: {file_type}"]
        return "\n".join(lines)