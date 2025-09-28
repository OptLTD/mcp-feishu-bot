#!/usr/bin/env python3
"""
Feishu MCP Server

A Model Context Protocol (MCP) server that integrates with Feishu (Lark) messaging platform.
Provides tools for sending messages, images, and files through Feishu API with auto long connection.
"""

import os
import atexit
import warnings
import json
from typing import Optional

# Additional runtime warning suppression as backup
warnings.filterwarnings("ignore", category=DeprecationWarning)

from fastmcp import FastMCP
from mcp_feishu_bot.drive import DriveHandle
from mcp_feishu_bot.client import FeishuClient
from mcp_feishu_bot.message import MessageHandle
from mcp_feishu_bot.bitable import BitableHandle

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

def main() -> None:
    """Entry point for console script to start MCP server.
    Intention: Provide a stable callable for packaging.
    """
    initialize_feishu_client()
    mcp.run(show_banner=False)


@mcp.tool
def chat_send_text(receive_id: str, content: str, receive_id_type: str = "email", msg_type: str = "text") -> str:
    """
    Send a message to a Feishu user or group.
    
    Args:
        receive_id: The ID of the message receiver (user_id, open_id, union_id, email, or chat_id)
        content: The message content (text or rich text format)
        msg_type: Message type (text, rich_text, etc.)
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        
    Returns:
        Markdown string containing the result of the message sending operation
    """
    global message_client
    
    if not message_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    return message_client.send_text_markdown(receive_id, content, msg_type, receive_id_type)


@mcp.tool
def chat_send_image(receive_id: str, image_path: str, receive_id_type: str = "email") -> str:
    """
    Send an image to a Feishu user or group.
    
    Args:
        receive_id: The ID of the message receiver
        image_path: Path to the image file to send
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        
    Returns:
        Markdown string containing the result of the image sending operation
    """
    global message_client
    
    if not message_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    return message_client.send_image_markdown(receive_id, image_path, receive_id_type)


@mcp.tool
def chat_send_file(receive_id: str, file_path: str, receive_id_type: str = "email", file_type: str = "stream") -> str:
    """
    Send a file to a Feishu user or group.
    
    Args:
        receive_id: The ID of the message receiver
        file_path: Path to the file to send
        file_type: Type of file (stream, opus, mp4, pdf, doc, xls, ppt, etc.)
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        
    Returns:
        Markdown string containing the result of the file sending operation
    """
    global message_client
    
    if not message_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    return message_client.send_file_markdown(receive_id, file_path, receive_id_type, file_type)


@mcp.tool
def bitable_list_tables(app_token: str, page_size: int = 20) -> str:
    """
    List all tables in a Feishu Bitable app and return Markdown describing
    each table and its fields.
    
    Args:
        app_token: The token of the bitable app
        page_size: Number of tables to return per page (default: 20)
        
    Returns:
        Markdown string containing the description of tables and fields
    """
    # Delegate to BitableHandle which encapsulates the Markdown generation
    bitable_handle = BitableHandle(app_token)
    return bitable_handle.describe_tables_markdown(page_size)


@mcp.tool
def bitable_list_records(app_token: str, table_id: str, options: dict) -> str:
    """
    List records in a Feishu Bitable table.
    
    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        page_size: Number of records to return per page (default: 20)
        
    Returns:
        Markdown string containing the list of records
    """
    global bitable_client
    
    if not bitable_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    # Create a new BitableHandle instance with the specific app_token and table_id
    bitable_handle = BitableHandle(app_token, table_id)

    # Parse options for pagination and query
    page_size = int(options.get("page_size", 20))
    page_index = int(options.get("page_index", 1))
    query = options.get("query", {}) or {}

    # Always return JSON-style per-record sections; formatting handled in bitable.py
    return bitable_handle.describe_records_markdown(page_size=page_size, page_index=page_index, query=query)


@mcp.tool
def bitable_query_record(app_token: str, table_id: str, record_id: str) -> str:
    """
    Get a specific record from a Feishu Bitable table.
    
    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        record_id: The ID of the record to retrieve
        
    Returns:
        Markdown string containing the record information
    """
    global bitable_client
    
    if not bitable_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    # Create a new BitableHandle instance with the specific app_token and table_id
    bitable_handle = BitableHandle(app_token, table_id)
    return bitable_handle.describe_record_markdown(record_id)


@mcp.tool
def bitable_upsert_record(app_token: str, table_id: str, fields: dict) -> str:
    """
    Update an existing record in a Feishu Bitable table, returning Markdown.
    The record_id must be provided inside the fields. If the record does not exist, return an error.

    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        fields: Dictionary of field values to update; MUST include 'record_id'
        
    Returns:
        Markdown string describing the update result or the error
    """
    global bitable_client
    
    if not bitable_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."

    # Validate record_id inside fields
    record_id = fields.get("record_id")
    if not record_id:
        return f"# error: missing record_id\ntable_id: {table_id}"

    # Prepare update fields (do not send record_id as a field to API)
    update_fields = {k: v for k, v in fields.items() if k != "record_id"}
    bitable_handle = BitableHandle(app_token, table_id)
    return bitable_handle.update_record_markdown(record_id, update_fields)


@mcp.tool
def drive_query_files(folder_token: str = "", options: dict = None) -> str:
    """
    List files in a Feishu Drive folder and return Markdown.
    Options dict follows bitable_list_records style: supports page_size, page_index, order_by, direction, user_id_type, and query for multi-condition matching.
    
    Args:
        folder_token: Token of the folder to list files from (empty for root directory)
        options: Dictionary with keys:
            - page_size: Number of items per page (default: 100, max: 200)
            - page_index: 1-based index of the page to fetch (default: 1)
            - order_by: Sort order (EditedTime or CreatedTime)
            - direction: Sort direction (ASC or DESC)
            - user_id_type: Type of user ID (open_id, union_id, user_id)
            - query: dict of field=value pairs to filter items (string equality; lists use containment)
    
    Returns:
        Markdown string containing the file list and pagination info
    """
    if not drive_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    options = options or {}
    return drive_client.describe_files_markdown(folder_token=folder_token, options=options)


@mcp.tool
def drive_delete_file(file_token: str, file_type: str) -> str:
    """
    Delete a file or folder in Feishu Drive
    
    Args:
        file_token: Token of the file or folder to delete
        file_type: Type of the file (file, docx, bitable, folder, doc)
    
    Returns:
        Markdown string containing the deletion result
    """
    if not drive_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    return drive_client.delete_file_markdown(file_token, file_type)


if __name__ == "__main__":
    # Allow direct execution via python -m or script run
    main()
