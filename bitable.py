#!/usr/bin/env python3
"""
Feishu Bitable Operations

Bitable (spreadsheet) operations for Feishu (Lark) API including:
- Create and manage bitables
- Read and write table data
- Manage table structure (fields, views)
"""

import warnings
from typing import Dict, Any, List, Optional

# Suppress deprecation warnings from lark_oapi library
warnings.filterwarnings("ignore", category=DeprecationWarning)

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import (
    ListAppTableRequest,
    ListAppTableRecordRequest,
    CreateAppTableRecordRequest,
    UpdateAppTableRecordRequest,
    DeleteAppTableRecordRequest,
    GetAppTableRecordRequest,
    ListAppTableFieldRequest,
    CreateAppTableFieldRequest,
    UpdateAppTableFieldRequest,
    DeleteAppTableFieldRequest,
    AppTableRecord
)

from client import FeishuClient


class BitableHandle(FeishuClient):
    """
    Feishu Bitable client with comprehensive spreadsheet functionality
    """
    
    def __init__(self, app_token: str, table_id: str = None):
        """
        Initialize BitableHandle with app_token and optional table_id
        
        Args:
            app_token: The token of the bitable app
            table_id: The ID of the table (optional, can be set later)
        """
        super().__init__()
        if not app_token:
            raise ValueError("app_token is required")
        self.app_token = app_token
        self.table_id = table_id
    
    def list_tables(self, page_size: int = 20, page_token: str = None) -> Dict[str, Any]:
        """
        List all tables in a bitable app
        
        Args:
            page_size: Number of tables to return per page
            page_token: Token for pagination
            
        Returns:
            Dictionary containing the list of tables
        """
        try:
            request = ListAppTableRequest.builder() \
                .app_token(self.app_token) \
                .page_size(page_size)
            
            if page_token:
                request = request.page_token(page_token)
                
            request = request.build()
            
            response = self.http_client.bitable.v1.app_table.list(request)
            
            if response.success():
                return {
                    "success": True,
                    "tables": [
                        {
                            "name": table.name,
                            "table_id": table.table_id,
                            "revision": table.revision
                        } for table in response.data.items
                    ],
                    "has_more": response.data.has_more,
                    "page_token": response.data.page_token
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to list tables: {response.msg}",
                    "code": response.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }
    
    def list_fields(self, page_size: int = 20, page_token: str = None) -> Dict[str, Any]:
        """
        List all fields in a table
        
        Args:
            page_size: Number of fields to return per page
            page_token: Token for pagination
            
        Returns:
            Dictionary containing the list of fields
        """
        if not self.table_id:
            return {
                "success": False,
                "error": "table_id is required either as parameter or instance variable"
            }
            
        try:
            request = ListAppTableFieldRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .page_size(page_size)
            
            if page_token:
                request = request.page_token(page_token)
                
            request = request.build()
            
            response = self.http_client.bitable.v1.app_table_field.list(request)
            
            if response.success():
                return {
                    "success": True,
                    "fields": [
                        {
                            "field_id": field.field_id,
                            "field_name": field.field_name,
                            "type": field.type,
                            "property": field.property,
                            "description": field.description
                        } for field in response.data.items
                    ],
                    "has_more": response.data.has_more,
                    "page_token": response.data.page_token,
                    "total": response.data.total
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to list fields: {response.msg}",
                    "code": response.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }
    
    def list_records(self, page_size: int = 20, page_token: str = None,
                    view_id: str = None, filter_condition: str = None,
                    sort: List[str] = None) -> Dict[str, Any]:
        """
        List records in a table
        
        Args:
            page_size: Number of records to return per page
            page_token: Token for pagination
            view_id: ID of the view to use
            filter_condition: Filter condition for records
            sort: List of sort conditions
            
        Returns:
            Dictionary containing the list of records
        """
        if not self.table_id:
            return {
                "success": False,
                "error": "table_id is required"
            }
            
        try:
            request = ListAppTableRecordRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .page_size(page_size)
            
            if page_token:
                request = request.page_token(page_token)
            if view_id:
                request = request.view_id(view_id)
            if filter_condition:
                request = request.filter(filter_condition)
            if sort:
                request = request.sort(sort)
                
            request = request.build()
            
            response = self.http_client.bitable.v1.app_table_record.list(request)
            
            if response.success():
                return {
                    "success": True,
                    "records": [
                        {
                            "record_id": record.record_id,
                            "fields": record.fields,
                            "created_by": record.created_by,
                            "created_time": record.created_time,
                            "last_modified_by": record.last_modified_by,
                            "last_modified_time": record.last_modified_time
                        } for record in response.data.items
                    ],
                    "has_more": response.data.has_more,
                    "page_token": response.data.page_token,
                    "total": response.data.total
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to list records: {response.msg}",
                    "code": response.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }
    
    def create_record(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new record in a table
        
        Args:
            fields: Dictionary of field values for the new record
            
        Returns:
            Dictionary containing the created record information
        """
        # Use provided table_id or fall back to instance table_id
        if not self.table_id:
            return {
                "success": False,
                "error": "table_id is required either as parameter or instance variable"
            }
            
        try:
            # Create record object
            record = AppTableRecord.builder().fields(fields).build()
            
            request = CreateAppTableRecordRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .request_body(record) \
                .build()
            
            response = self.http_client.bitable.v1.app_table_record.create(request)
            
            if response.success():
                return {
                    "success": True,
                    "record_id": response.data.record.record_id,
                    "fields": response.data.record.fields,
                    "created_by": response.data.record.created_by,
                    "created_time": response.data.record.created_time
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to create record: {response.msg}",
                    "code": response.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }
    
    def update_record(self, record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update an existing record in a table
        
        Args:
            record_id: The ID of the record to update
            fields: Dictionary of field values to update
            table_id: The ID of the table (optional, uses instance table_id if not provided)
            
        Returns:
            Dictionary containing the updated record information
        """
        if not self.table_id:
            return {
                "success": False,
                "error": "table_id is required either as parameter or instance variable"
            }
            
        try:
            # Create record object with updated fields
            record = AppTableRecord.builder().fields(fields).build()
            
            request = UpdateAppTableRecordRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .record_id(record_id) \
                .request_body(record) \
                .build()
            
            response = self.http_client.bitable.v1.app_table_record.update(request)
            
            if response.success():
                return {
                    "success": True,
                    "record_id": response.data.record.record_id,
                    "fields": response.data.record.fields,
                    "last_modified_by": response.data.record.last_modified_by,
                    "last_modified_time": response.data.record.last_modified_time
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to update record: {response.msg}",
                    "code": response.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }
    
    def delete_record(self, record_id: str) -> Dict[str, Any]:
        """
        Delete a record from a table
        
        Args:
            record_id: The ID of the record to delete
            table_id: The ID of the table (optional, uses instance table_id if not provided)
            
        Returns:
            Dictionary containing the deletion result
        """
        if not self.table_id:
            return {
                "success": False,
                "error": "table_id is required either as parameter or instance variable"
            }
            
        try:
            request = DeleteAppTableRecordRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .record_id(record_id) \
                .build()
            
            response = self.http_client.bitable.v1.app_table_record.delete(request)
            
            if response.success():
                return {
                    "success": True,
                    "deleted": response.data.deleted
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to delete record: {response.msg}",
                    "code": response.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }
    
    def query_record(self, record_id: str) -> Dict[str, Any]:
        """
        Get a specific record from a table
        
        Args:
            record_id: The ID of the record to retrieve
            
        Returns:
            Dictionary containing the record information
        """
        if not self.table_id:
            return {
                "success": False,
                "error": "table_id is required either as parameter or instance variable"
            }
            
        try:
            request = GetAppTableRecordRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .record_id(record_id) \
                .build()
            
            response = self.http_client.bitable.v1.app_table_record.get(request)
            
            if response.success():
                record = response.data.record
                return {
                    "success": True,
                    "record_id": record.record_id,
                    "fields": record.fields,
                    "created_by": record.created_by,
                    "created_time": record.created_time,
                    "last_modified_by": record.last_modified_by,
                    "last_modified_time": record.last_modified_time
                }
            else:
                return {
                    "success": False,
                    "error": f"Failed to get record: {response.msg}",
                    "code": response.code
                }
                
        except Exception as e:
            return {
                "success": False,
                "error": f"Exception occurred: {str(e)}"
            }
    
