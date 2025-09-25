#!/usr/bin/env python3
"""
Feishu MCP Server

A Model Context Protocol (MCP) server that integrates with Feishu (Lark) messaging platform.
Provides tools for sending messages, images, and files through Feishu API with auto long connection.
"""

import os
import atexit
import warnings
from typing import Optional

# Additional runtime warning suppression as backup
warnings.filterwarnings("ignore", category=DeprecationWarning)

from fastmcp import FastMCP
from drive import DriveHandle
from client import FeishuClient
from message import MessageHandle
from bitable import BitableHandle

# Initialize FastMCP server
mcp = FastMCP("Feishu MCP Server")

# Initialize global Feishu clients
drive_client: Optional[DriveHandle] = None
feishu_client: Optional[FeishuClient] = None
message_client: Optional[MessageHandle] = None
bitable_client: Optional[BitableHandle] = None

def initialize_feishu_client() -> Optional[FeishuClient]:
    """
    Initialize Feishu clients with environment variables and start long connection.
    Returns None if required environment variables are not set.
    """
    global feishu_client, message_client, bitable_client, drive_client
    
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    
    if not app_id or not app_secret:
        print("[Warning] FEISHU_APP_ID and FEISHU_APP_SECRET not configured")
        return None
    
    try:
        # Initialize base client for connection management
        feishu_client = FeishuClient(app_id, app_secret)
        
        # Initialize specialized clients
        message_client = MessageHandle(app_id, app_secret)
        bitable_client = BitableHandle(app_id, app_secret)
        drive_client = DriveHandle(app_id, app_secret)
        
        # Auto-start long connection when server initializes
        if feishu_client.start_long_connection():
            print("[Info] Feishu long connection started successfully")
        else:
            print("[Warning] Failed to start Feishu long connection")
        return feishu_client
    except Exception as e:
        print(f"[Error] Failed to initialize Feishu client: {str(e)}")
        return None

def cleanup_feishu_client():
    """
    Cleanup function to stop long connection when server shuts down.
    """
    global feishu_client
    if feishu_client and feishu_client.is_connected():
        feishu_client.stop_long_connection()
        print("[Info] Feishu long connection stopped")

# Register cleanup function to run on exit
atexit.register(cleanup_feishu_client)


@mcp.tool
def send_text(receive_id: str, content: str, receive_id_type: str = "email", msg_type: str = "text") -> dict:
    """
    Send a message to a Feishu user or group.
    
    Args:
        receive_id: The ID of the message receiver (user_id, open_id, union_id, email, or chat_id)
        content: The message content (text or rich text format)
        msg_type: Message type (text, rich_text, etc.)
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        
    Returns:
        Dictionary containing the result of the message sending operation
    """
    global message_client
    
    if not message_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    return message_client.send_text(receive_id, content, msg_type, receive_id_type)


@mcp.tool
def send_image(receive_id: str, image_path: str, receive_id_type: str = "email") -> dict:
    """
    Send an image to a Feishu user or group.
    
    Args:
        receive_id: The ID of the message receiver
        image_path: Path to the image file to send
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        
    Returns:
        Dictionary containing the result of the image sending operation
    """
    global message_client
    
    if not message_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    return message_client.send_image(receive_id, image_path, receive_id_type)


@mcp.tool
def send_file(receive_id: str, file_path: str, receive_id_type: str = "email", file_type: str = "stream") -> dict:
    """
    Send a file to a Feishu user or group.
    
    Args:
        receive_id: The ID of the message receiver
        file_path: Path to the file to send
        file_type: Type of file (stream, opus, mp4, pdf, doc, xls, ppt, etc.)
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        
    Returns:
        Dictionary containing the result of the file sending operation
    """
    global message_client
    
    if not message_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    return message_client.send_file(receive_id, file_path, receive_id_type, file_type)


@mcp.tool
def hello(name: str = "World") -> str:
    """
    A simple hello world function.
    
    Args:
        name: Name to greet (defaults to "World")
        
    Returns:
        A greeting message
    """
    return f"Hello, {name}! This is the Feishu MCP Server."

@mcp.tool
def query_drive_files(folder_token: str = "", page_size: int = 100, 
                    page_token: str = "", order_by: str = "EditedTime",
                    direction: str = "DESC") -> dict:
    """
    List files and folders in a Feishu Drive directory.
    
    Args:
        folder_token: Token of the folder to list files from (empty for root directory)
        page_size: Number of items per page (default: 100, max: 200)
        page_token: Pagination token for next page
        order_by: Sort order (EditedTime or CreatedTime)
        direction: Sort direction (ASC or DESC)
        
    Returns:
        Dictionary containing the file list and pagination info
    """
    global drive_client
    if not drive_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    return drive_client.list_files(folder_token, page_size, page_token, order_by, direction)

# Bitable (Spreadsheet) Tools

@mcp.tool
def list_bitable_tables(app_token: str, page_size: int = 20) -> dict:
    """
    List all tables in a Feishu Bitable app.
    
    Args:
        app_token: The token of the bitable app
        page_size: Number of tables to return per page (default: 20)
        
    Returns:
        Dictionary containing the list of tables
    """
    bitable_handle = BitableHandle(app_token)
    return bitable_handle.list_tables(page_size)


@mcp.tool
def list_bitable_records(app_token: str, table_id: str, page_size: int = 20) -> dict:
    """
    List records in a Feishu Bitable table.
    
    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        page_size: Number of records to return per page (default: 20)
        
    Returns:
        Dictionary containing the list of records
    """
    global bitable_client
    
    if not bitable_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    # Create a new BitableHandle instance with the specific app_token and table_id
    bitable_handle = BitableHandle(app_token, table_id)
    return bitable_handle.list_records(page_size)


@mcp.tool
def create_bitable_record(app_token: str, table_id: str, fields: dict) -> dict:
    """
    Create a new record in a Feishu Bitable table.
    
    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        fields: Dictionary of field values for the new record
        
    Returns:
        Dictionary containing the created record information
    """
    global bitable_client
    
    if not bitable_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    # Create a new BitableHandle instance with the specific app_token and table_id
    bitable_handle = BitableHandle(app_token, table_id)
    return bitable_handle.create_record(fields)


@mcp.tool
def update_bitable_record(app_token: str, table_id: str, record_id: str, fields: dict) -> dict:
    """
    Update an existing record in a Feishu Bitable table.
    
    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        record_id: The ID of the record to update
        fields: Dictionary of field values to update
        
    Returns:
        Dictionary containing the updated record information
    """
    global bitable_client
    
    if not bitable_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    # Create a new BitableHandle instance with the specific app_token and table_id
    bitable_handle = BitableHandle(app_token, table_id)
    return bitable_handle.update_record(record_id, fields)


@mcp.tool
def query_bitable_record(app_token: str, table_id: str, record_id: str) -> dict:
    """
    Get a specific record from a Feishu Bitable table.
    
    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        record_id: The ID of the record to retrieve
        
    Returns:
        Dictionary containing the record information
    """
    global bitable_client
    
    if not bitable_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    # Create a new BitableHandle instance with the specific app_token and table_id
    bitable_handle = BitableHandle(app_token, table_id)
    return bitable_handle.query_record(record_id)


@mcp.tool
def query_drive_files(folder_token: str = "", page_size: int = 100, page_token: str = "", 
                    order_by: str = "EditedTime", direction: str = "DESC", 
                    user_id_type: str = "open_id") -> dict:
    """
    List files in a Feishu Drive folder
    
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
    if not drive_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    return drive_client.list_files(folder_token, page_size, page_token, order_by, direction, user_id_type)


@mcp.tool
def delete_drive_file(file_token: str, file_type: str) -> dict:
    """
    Delete a file or folder in Feishu Drive
    
    Args:
        file_token: Token of the file or folder to delete
        file_type: Type of the file (file, docx, bitable, folder, doc)
    
    Returns:
        Dictionary containing the deletion result
    """
    if not drive_client:
        return {
            "success": False,
            "error": "Feishu client not configured. Please set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
        }
    
    return drive_client.delete_file(file_token, file_type)


if __name__ == "__main__":
    # Initialize Feishu client on startup
    initialize_feishu_client()
    
    # Start the MCP server
    mcp.run(show_banner=False)
