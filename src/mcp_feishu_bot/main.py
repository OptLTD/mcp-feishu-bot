#!/usr/bin/env python3
"""
Feishu MCP Server

A Model Context Protocol (MCP) server that integrates with Feishu (Lark) messaging platform.
Provides tools for sending messages, images, and files through Feishu API with auto long connection.
"""

import os, atexit, warnings 
import logging
from logging.handlers import RotatingFileHandler
from typing import Optional

# Additional runtime warning suppression as backup
warnings.filterwarnings("ignore", category=DeprecationWarning)

from fastmcp import FastMCP
from .msg import MsgHandle
from .wiki import WikiHandle
from .drive import DriveHandle
from .robot import RobotClient
from .relay import RelayHandle
from .client import FeishuClient
from .bitable import BitableHandle
from fastmcp.utilities.logging import get_logger

# Initialize FastMCP server
mcp = FastMCP("Feishu MCP Server")

# Module logger
logger = get_logger(__name__)

# Initialize global Feishu clients
msg_client: Optional[MsgHandle] = None
wiki_client: Optional[WikiHandle] = None
relay_client: Optional[RelayHandle] = None
drive_client: Optional[DriveHandle] = None
robot_client: Optional[RobotClient] = None
feishu_client: Optional[FeishuClient] = None
bitable_clients: Optional[dict[str, BitableHandle]] = {}

def initialize_agent_client(relay_handle: RelayHandle) -> Optional[RobotClient]:
    global robot_client
   
    # Guard against None for msg_server to avoid AttributeError
    if robot_host := os.getenv("FEISHU_ROBOT_HOST"):
        robot_client = RobotClient(
            host=robot_host, reconnect=True,
            on_event=relay_handle.on_robot_event,
        )
        robot_client.start()
        logger.info("Robot WS long connection started")
    else:
        logger.info("Robot WS long connection not started")

    relay_handle.set_robot(robot_client)
    return robot_client

def cleanup_agent_client():
    global robot_client
    try:
        if robot_client:
            robot_client.stop()
            logger.info("Agent WS long connection stopped")
    except Exception as e:
        logger.warning(f"Failed to stop Agent WS: {e}")

def initialize_feishu_client(relay_handle: RelayHandle) -> Optional[FeishuClient]:
    global feishu_client, msg_client, drive_client, wiki_client 
    
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    if not app_id or not app_secret:
        logger.warning("FEISHU_APP_ID and FEISHU_APP_SECRET not configured")
        return None
    
    try:
        feishu_client = FeishuClient(
            app_id=app_id, app_secret=app_secret, 
            on_event=relay_handle.on_feishu_msg,
        )
        # Initialize specialized clients
        msg_client = MsgHandle(app_id, app_secret)
        drive_client = DriveHandle(app_id, app_secret)
        wiki_client = WikiHandle(app_id, app_secret)
    except Exception as e:
        logger.error(f"Failed to initialize Feishu client: {str(e)}")
        return None
    
    # Start long connection if enabled
    if os.getenv("FEISHU_ROBOT_HOST") != '':
        # Auto-start long connection when server initializes
        if feishu_client.start_long_connection():
            logger.info("Feishu long connection started successfully")
        else:
            logger.warning("Failed to start Feishu long connection")

    # finally set the msg_client to relay_handle
    relay_handle.set_feishu(msg_client)
    return feishu_client

def cleanup_feishu_client():
    global feishu_client
    if feishu_client and feishu_client.is_connected():
        feishu_client.stop_long_connection()
        logger.info("Feishu long connection stopped")

# Register cleanup function to run on exit
atexit.register(cleanup_feishu_client)
atexit.register(cleanup_agent_client)

def setup_file_logging() -> None:
    """Configure rotating file logging for the application."""
    # Prefer STOARGE_PATH/.logs if STOARGE_PATH is set; otherwise use ~/.logs
    if storage_path := os.environ.get("STOARGE_PATH"):
        log_dir = os.path.join(storage_path, ".logs")
    else:
        log_dir = os.path.join(os.path.expanduser("~"), ".logs")
    try:
        os.makedirs(log_dir, exist_ok=True)
    except Exception:
        pass
    log_file = os.path.join(log_dir, "mcp-feishu-bot.log")

    root_logger = logging.getLogger()
    # Default to INFO if not specified
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    root_logger.setLevel(level)

    # Remove StreamHandlers to avoid writing to STDOUT which may corrupt MCP JSON frames
    try:
        for h in list(root_logger.handlers):
            if isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler):
                root_logger.removeHandler(h)
    except Exception:
        pass

    # Avoid adding duplicate handlers for the same file
    exists = False
    for h in root_logger.handlers:
        if isinstance(h, RotatingFileHandler):
            try:
                if os.path.abspath(getattr(h, "baseFilename", "")) == os.path.abspath(log_file):
                    exists = True
                    break
            except Exception:
                continue
    if not exists:
        fh = RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        fh.setFormatter(fmt)
        fh.setLevel(level)
        root_logger.addHandler(fh)

def main() -> None:
    setup_file_logging()
    relay_handle = RelayHandle()
    initialize_agent_client(relay_handle)
    initialize_feishu_client(relay_handle)
    mcp.run(show_banner=False)


@mcp.tool
def chat_send_text(receive_id: str, content: str, receive_id_type: str = "email") -> str:
    """
    [Feishu/Lark] Send a message to a user or group.
    
    Args:
        receive_id: The ID of the message receiver (user_id, open_id, union_id, email, or chat_id)
        content: The message content (text or rich text format)
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        
    Returns:
        Markdown string containing the result of the message sending operation
    """
    global msg_client
    
    if not msg_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    try:
        resp = msg_client.send_text(
            receive_id=receive_id, content=content, 
            receive_id_type=receive_id_type
        )
        if not resp.success():
            return f"# error: Failed to send message: {resp.error}"
        return "# ok: Message sent successfully"
    except Exception as e:
        return f"# error: Failed to send message: {str(e)}"


@mcp.tool
def chat_send_image(receive_id: str, image_path: str, receive_id_type: str = "email") -> str:
    """
    [Feishu/Lark] Send an image to a user or group.
    
    Args:
        receive_id: The ID of the message receiver
        image_path: Path to the image file to send
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        
    Returns:
        Markdown string containing the result of the image sending operation
    """
    global msg_client
    
    if not msg_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    try:
        resp = msg_client.send_image(
             receive_id=receive_id, image_path=image_path, 
            receive_id_type=receive_id_type
        )
        if not resp.success():
            return f"# error: Failed to send image: {resp.error}"
        return "# ok: Image sent successfully"
    except Exception as e:
        return f"# error: Failed to send image: {str(e)}"


@mcp.tool
def chat_send_file(receive_id: str, file_path: str, receive_id_type: str = "email", file_type: str = "stream") -> str:
    """
    [Feishu/Lark] Send a file to a Feishu user or group.
    
    Args:
        receive_id: The ID of the message receiver
        file_path: Path to the file to send
        file_type: Type of file (stream, opus, mp4, pdf, doc, xls, ppt, etc.)
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        
    Returns:
        Markdown string containing the result of the file sending operation
    """
    global msg_client
    
    if not msg_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    try:
        resp = msg_client.send_file(
            receive_id=receive_id, receive_id_type=receive_id_type,
            file_path=file_path,  file_type=file_type
        )
        if not resp.success():
            return f"# error: Failed to send file: {resp.error}"
        return "# ok: File sent successfully"
    except Exception as e:
        return f"# error: Failed to send file: {str(e)}"


@mcp.tool
def chat_send_card(receive_id: str, content: dict, receive_id_type: str = "email") -> str:
    """
    [Feishu/Lark] Send an interactive card message.

    Args:
        receive_id: The ID of the message receiver (user_id, open_id, union_id, email, chat_id)
        receive_id_type: Type of receiver ID (open_id, user_id, union_id, email, chat_id)
        content: The interactive card content(dict object),cart content just like this: {
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

    Returns:
        Markdown string containing the result of the message sending operation
    """
    global msg_client

    if not msg_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."

    try:
        resp = msg_client.send_card(
            receive_id=receive_id, content=content,
            receive_id_type=receive_id_type,
        )
        if not resp.success():
            return f"# error: Failed to send card: {resp.error}"
        return "# ok: Card message sent successfully"
    except Exception as e:
        return f"# error: Failed to send card: {str(e)}"


@mcp.tool
def drive_query_files(folder_token: str = "", options: dict = None) -> str:
    """
    [Feishu/Lark] List files in a Drive folder and return Markdown.
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
    [Feishu/Lark] Delete a file or folder in Feishu Drive
    
    Args:
        file_token: Token of the file or folder to delete
        file_type: Type of the file (file, docx, bitable, folder, doc)
    
    Returns:
        Markdown string containing the deletion result
    """
    if not drive_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    return drive_client.delete_file_markdown(file_token, file_type)


@mcp.tool
def bitable_list_tables(app_token: str, page_size: int = 50) -> str:
    """
    [Feishu/Lark] List all tables in a Bitable app and return Markdown describing
    each table and its fields.
    
    Args:
        app_token: The token of the bitable app
        page_size: Number of tables to return per page (default: 20)
        
    Returns:
        Markdown string containing the description of tables and fields
    """
    # Delegate to BitableHandle which encapsulates the Markdown generation
    if app_token not in bitable_clients:
        bitable_clients[app_token] = BitableHandle(app_token)
    bitable_handle = bitable_clients[app_token]
    return bitable_handle.describe_tables(page_size)


@mcp.tool
def bitable_list_records(app_token: str, table_id: str, options: dict = {}) -> str:
    """
    [Feishu/Lark] List records in a Bitable table.
    
    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        options: Dictionary of pagination and query options (default: {})
        
    Returns:
        Markdown string containing the list of records
    """
    global bitable_clients

    if app_token not in bitable_clients:
        bitable_clients[app_token] = BitableHandle(app_token)
    bitable_handle = bitable_clients[app_token].use_table(table_id)
    # Parse options for pagination and query
    page_size = int(options.get("page_size", 20))
    page_token = options.get("page_token", None)

    # Always return JSON-style per-record sections; formatting handled in bitable.py
    return bitable_handle.describe_list_records(page_size=page_size, page_token=page_token)

@mcp.tool
def bitable_search_records(app_token: str, table_id: str, query: dict, options: dict = None) -> str:
    """
    [Feishu/Lark] Search records in a Bitable table with simplified field-based filtering.
    
    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        query: Simple query object with field names as keys and values/arrays as values. Format:
            {
                "field_name1": "single_value",
                "field_name2": ["value1", "value2"],  # Array for multiple values
                "field_name3": ["record_id"]  # Record references
            }
        options: Dictionary of additional options (default: None)
            - sorts: List of sort conditions [{"field_name": "name", "desc": false}]
            - page_size: Number of records per page (max 100, default 20)
            - page_token: Token for pagination
        
    Returns:
        Markdown string containing the query results
    """
    global bitable_clients
    
    if not feishu_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    # Create or get BitableHandle instance for the app_token
    if app_token not in bitable_clients:
        bitable_clients[app_token] = BitableHandle(app_token)
    
    bitable_handle = bitable_clients[app_token].use_table(table_id)
    
    # Parse options
    options = options or {}
    sorts = options.get("sorts")
    page_size = int(options.get("page_size", 20))
    page_token = options.get("page_token")
    return bitable_handle.describe_search_records(
        query=query, sorts=sorts,
        page_size=page_size,
        page_token=page_token
    )

@mcp.tool
def bitable_find_record(app_token: str, table_id: str, record_id: str) -> str:
    """
    [Feishu/Lark] Get a specific record from a Bitable table.
    
    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        record_id: The ID of the record to retrieve
        
    Returns:
        Markdown string containing the record information
    """
    global bitable_clients
    
    if not feishu_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    
    # Create or get BitableHandle instance for the app_token
    if app_token not in bitable_clients:
        bitable_clients[app_token] = BitableHandle(app_token)
    
    bitable_handle = bitable_clients[app_token].use_table(table_id)
    return bitable_handle.describe_query_record(record_id)


@mcp.tool
def bitable_upsert_record(app_token: str, table_id: str, fields: dict) -> str:
    """
    [Feishu/Lark] Upsert a record in a Bitable table, returning Markdown.
    Uses enhanced field processing to handle related fields automatically.
    
    Logic:
    1. If record_id is provided, use update logic
    2. If no record_id, use first field as index field to match existing record
    3. For related fields, match records in related tables using record_id or index field, create if not found
    4. Datetime fields must be timestamp in milliseconds or ISO 8601 format (e.g., "2023-01-01T00:00:00Z")

    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        fields: Dictionary of field values; may include 'record_id' for direct update
        
    Returns:
        Markdown string describing the upsert result or the error
    """
    bitable_handle = BitableHandle(app_token, table_id)
    return bitable_handle.describe_upsert_record(fields)


@mcp.tool
def bitable_delete_record(app_token: str, table_id: str, record_id: str) -> str:
    """
    [Feishu/Lark] Delete a specific record in a Bitable table.

    Args:
        app_token: The token of the bitable app
        table_id: The ID of the table
        record_id: The ID of the record to delete

    Returns:
        Markdown string describing the deletion result or the error
    """
    global bitable_clients

    if not feishu_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."

    if app_token not in bitable_clients:
        bitable_clients[app_token] = BitableHandle(app_token)
    bitable_handle = bitable_clients[app_token].use_table(table_id)
    return bitable_handle.describe_delete_record(record_id)

# -------------------- Bitable Field Tools --------------------
@mcp.tool
def bitable_create_table(app_token: str, table_name: str, fields: list[dict] = None) -> str:
    """
    [Feishu/Lark] Create a Bitable table by `table_name` and `fields`.

    Logic:
    - create a new table with optional initial fields.
    - field properties: `field_name`, `type` required. 
    - field type enum: 1: 文本, 2: 数字, 3: 日期, 4: 单选, 5: 多选, 7: 人员, 11: 链接, 13: 附件, 15: 状态, 17: 进度, 18: 倒计时, 20: 地理位置, 21: 单选（关联）, 22: 多选（关联）, 23: 自动编号, 1001: 长文本, 1002: 多行文本, 1003: 邮箱, 1004: 手机号, 1005: 组织架构

    Args:
        app_token: Bitable app token
        table_name: Target table name to create
        fields: Optional list of field definitions to create

    Returns:
        Markdown describing table create table with fields results
    """
    if app_token not in bitable_clients:
        bitable_clients[app_token] = BitableHandle(app_token)
    bitable_handle = bitable_clients[app_token]
    return bitable_handle.describe_create_table(table_name, fields)

@mcp.tool
def bitable_query_fields(app_token: str, table_id: str) -> str:
    """
    [Feishu/Lark] Retrieve all fields of a given table and return Markdown.

    Args:
        app_token: Bitable app token
        table_id: Target table ID

    Returns:
        Markdown string describing field details and properties
    """
    if app_token not in bitable_clients:
        bitable_clients[app_token] = BitableHandle(app_token)
    bitable_handle = bitable_clients[app_token].use_table(table_id)
    return bitable_handle.describe_query_fields(table_id)


@mcp.tool
def bitable_upsert_fields(app_token: str, table_id: str, fields: list[dict]) -> str:
    """
    [Feishu/Lark] Batch upsert fields (create or update) and return a Markdown result.

    Rules:
    - field properties: `field_name`, `type` required.
    - field type defined ref bitable_create_table field type enum.
    - If field exists, update it, otherwise create a new field.
    - If field is a related field, it must reference an existing table.
    - If field is a single-select or multi-select field, it must have options.

    Args:
        app_token: Bitable app token
        table_id: Target table ID
        fields: List of field definitions

    Returns:
        Markdown string with the result of each field operation
    """
    if app_token not in bitable_clients:
        bitable_clients[app_token] = BitableHandle(app_token)
    bitable_handle = bitable_clients[app_token].use_table(table_id)
    return bitable_handle.describe_upsert_fields(fields)


@mcp.tool
def bitable_delete_fields(app_token: str, table_id: str, field_ids: list[str] = None) -> str:
    """
    [Feishu/Lark] Batch delete fields using `field_ids` only.

    Args:
        app_token: Bitable app token
        table_id: Target table ID
        field_ids: List of field IDs to delete

    Returns:
        Markdown string describing deletion results
    """
    if app_token not in bitable_clients:
        bitable_clients[app_token] = BitableHandle(app_token)
    bitable_handle = bitable_clients[app_token].use_table(table_id)
    return bitable_handle.describe_delete_fields(field_ids=field_ids)

@mcp.tool
def wiki_doc_content(doc_token: str) -> str:
    """
    [Feishu/Lark] Get document content by token and return Markdown.

    Args:
        doc_token: Token of the document

    Returns:
        Markdown string containing the document content
    """
    if not wiki_client:
        return "# error: Feishu client not configured\nPlease set FEISHU_APP_ID and FEISHU_APP_SECRET environment variables."
    return wiki_client.describe_get_content(doc_token=doc_token)


if __name__ == "__main__":
    # Allow direct execution via python -m or script run
    main()