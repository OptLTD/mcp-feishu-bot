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

from client import FeishuClient


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