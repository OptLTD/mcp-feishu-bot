#!/usr/bin/env python3

import warnings, json
from typing import Dict, Any, List, Optional, Tuple
from fastmcp.utilities.logging import get_logger

logger = get_logger(__name__)

# Suppress deprecation warnings from lark_oapi library
warnings.filterwarnings("ignore", category=DeprecationWarning)

import lark_oapi as lark
from lark_oapi.api.bitable.v1 import (
    AppTable, AppTableField,
    AppTableRecord, AppTableView,
)
from lark_oapi.api.bitable.v1 import (
    ListAppTableRequest,
    ListAppTableFieldRequest,
    ListAppTableRecordRequest,
    CreateAppTableRecordRequest,
    UpdateAppTableRecordRequest,
    DeleteAppTableRecordRequest,
    GetAppTableRecordRequest,
    SearchAppTableRecordRequest,
    CreateAppTableFieldRequest,
    UpdateAppTableFieldRequest,
    DeleteAppTableFieldRequest,
    CreateAppTableRecordResponse,
    SearchAppTableRecordResponse,
)

from mcp_feishu_bot.client import FeishuClient


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
        
        # Cache attributes to store app-level data
        # Purpose: Avoid repeated API calls by caching tables, fields, and views information
        self._cached_tables: Optional[List[Dict[str, Any]]] = None
        self._cached_fields: Dict[str, List[Dict[str, Any]]] = {}
        self._cached_views: Dict[str, List[Dict[str, Any]]] = {}
    
    def use_table(self, table_id: str) -> 'BitableHandle':
        """
        Set the table_id to use for subsequent operations
        
        Args:
            table_id: The ID of the table to use
            
        Returns:
            Self for method chaining
        """
        self.table_id = table_id
        return self
    
    def get_cached_tables(self, page_size: int = 50) -> List[Dict[str, Any]]:
        """
        Get cached tables information, fetch from API if not cached
        
        Purpose: Provide access to tables data, automatically fetching if not cached
        
        Returns:
            List of cached tables
        """
        if self._cached_tables is None:
            self._cached_tables = self.get_remote_tables(
                page_size=page_size,
            )
        return self._cached_tables
    
    def get_cached_fields(self, table_id: str = None, page_size: int = 50) -> List[Dict[str, Any]]:
        """
        Get cached fields information for a specific table, fetch from API if not cached
        
        Purpose: Provide access to fields data, automatically fetching if not cached
        
        Args:
            table_id: The table ID to get fields for (uses instance table_id if not provided)
            
        Returns:
            List of cached fields
        """
        target_table_id = table_id or self.table_id
        if not target_table_id:
            raise ValueError("table_id is required either as parameter or instance variable")
        
        if target_table_id not in self._cached_fields:
            self._cached_fields[target_table_id] = self.get_remote_fields(
                table_id=target_table_id, page_size=page_size,
            )
        return self._cached_fields[target_table_id]
    
    def get_cached_views(self, table_id: str = None) -> List[Dict[str, Any]]:
        """
        Get cached views information for a specific table, fetch from API if not cached
        
        Purpose: Provide access to views data, automatically fetching if not cached
        
        Args:
            table_id: The table ID to get views for (uses instance table_id if not provided)
            
        Returns:
            List of cached views
        """
        target_table_id = table_id or self.table_id
        if not target_table_id:
            raise ValueError("table_id is required either as parameter or instance variable")
        
        if target_table_id not in self._cached_views:
            self._cached_views[target_table_id] = self.get_remote_views(target_table_id)
        
        return self._cached_views[target_table_id]

    def get_remote_tables(self, page_size: int = 50) -> List[AppTable]:
        """
        Fetch tables from API and return raw data objects
        
        Purpose: Fetch tables from Feishu API and cache raw response objects
        
        Args:
            page_size: Number of tables to return per page

        Returns:
            List of raw table objects from API response
        """

            
        all_tables = []
        page_token = None
        
        # Iterate through all tables using pagination
        while True:
            try:
                request = ListAppTableRequest.builder() \
                    .app_token(self.app_token) \
                    .page_size(page_size)
                if page_token:
                    request = request.page_token(page_token)
                response = self.http_client.bitable.v1.app_table.list(
                    request.build()
                )
                
                if response.success():
                    all_tables.extend(response.data.items)
                    # Check if there are more pages
                    if response.data.has_more:
                        page_token = response.data.page_token
                    else:
                        break
                else:
                    raise Exception(f"Failed to list tables: {response.msg} (code: {response.code})")
            except Exception as e:
                logger.error(f"Exception occurred while fetching tables: {str(e)}")
                raise
        return all_tables
 
    def get_remote_fields(self, table_id: str = None, page_size: int = 20) -> List[AppTableField]:
        """
        Fetch all fields from API and return raw data objects
        
        Purpose: Fetch fields from Feishu API and cache raw response objects
        
        Args:
            table_id: The table ID to get fields for (uses instance table_id if not provided)
            page_size: Number of fields to return per page
            
        Returns:
            List of raw field objects from API response
        """
        target_table_id = table_id or self.table_id
        if not target_table_id:
            raise ValueError("table_id is required either as parameter or instance variable")

        try:
            # Fetch all fields with pagination
            all_fields = []
            page_token = None
            
            while True:
                request = ListAppTableFieldRequest.builder() \
                    .app_token(self.app_token) \
                    .table_id(target_table_id) \
                    .page_size(page_size)
                if page_token:
                    request = request.page_token(page_token)
                response = self.http_client.bitable.v1.app_table_field.list(
                    request.build()
                )
                if response.success():
                    fields = list(response.data.items)
                    all_fields.extend(fields)
                    
                    # Check if there are more pages
                    if response.data.has_more and response.data.page_token:
                        page_token = response.data.page_token
                    else:
                        break
                else:
                    raise Exception(f"Failed to list fields: {response.msg} (code: {response.code})")
            return all_fields
        except Exception as e:
            logger.error(f"Exception occurred while fetching fields: {str(e)}")
            raise Exception(f"Exception occurred while fetching fields: {str(e)}")

    def get_remote_views(self, table_id: str = None) -> List[AppTableView]:
        """
        Fetch all views from API and return raw data objects
        
        Purpose: Fetch views from Feishu API and cache raw response objects
        Note: Currently returns empty list as view API is not implemented
        
        Args:
            table_id: The table ID to get views for (uses instance table_id if not provided)
            
        Returns:
            List of raw view objects from API response
        """
        target_table_id = table_id or self.table_id
        if not target_table_id:
            raise ValueError("table_id is required either as parameter or instance variable")
        
        # TODO: Implement view API calls when available
        # For now, return empty list and cache it
        views = []
        return views

    def describe_fields(self, table_id: str = None) -> str:
        """
        Describe all fields in a table, returning detailed field information in Markdown.
        
        Purpose: Transform field data into human-readable Markdown format
        
        Args:
            table_id: Target table ID (uses self.table_id if not provided)
            
        Returns:
            Markdown string describing all fields with their types and properties
        """
        target_table_id = table_id or self.table_id
        if not target_table_id:
            return "# error: table_id is required"
            
        try:
            fields = self.get_cached_fields(target_table_id)
        except Exception as e:
            return f"# error: {str(e)}\ntable_id: {target_table_id}"
        if not fields:
            return f"# No fields found\ntable_id: {target_table_id}"
        
        lines = [f"# Fields in table {target_table_id}", ""]
        for field in fields:
            field_name = field.field_name or "Unknown"
            field_type = field.type or "Unknown"
            field_id = field.field_id or "Unknown"
            description = field.description or ""
            property_info = field.property or {}
            
            lines.append(f"## {field_name}")
            lines.append(f"- **Field ID**: {field_id}")
            lines.append(f"- **Type**: {field_type}")
            if description:
                lines.append(f"- **Description**: {description}")
            if property_info:
                lines.append(f"- **Properties**: {json.dumps(property_info, ensure_ascii=False, indent=2)}")
            lines.append("")
        
        return "\n".join(lines)

    def describe_tables(self, page_size: int = 50) -> str:
        """
        Generate Markdown describing all tables and their fields within the bitable app.
        Always returns a Markdown string. Errors are returned as Markdown with a heading and details.
        
        Purpose: Cache all tables, fields, and views information during execution for later use.

        Args:
            page_size: Number of tables to return per page (default: 50)

        Returns:
            Markdown string containing the description of tables and fields
        """
        
        # Map field type codes to human-readable names (best-effort)
        # Note: Mapping is not exhaustive and can be extended based on API docs.
        type_map = {
            1: "文本",
            2: "数字",
            3: "单选",
            4: "多选",
            5: "日期",
            6: "复选框",
            7: "用户",
            8: "附件",
            9: "公式",
            10: "序列号",
            11: "链接",
            12: "邮件",
            13: "电话",
            14: "时间",
            17: "附件",
            18: "关联表",
            19: "查找",
            1005: "编号"
        }

        markdown_sections: list[str] = []

        # Utility: safely access properties that may be dicts or SDK objects
        # Purpose: Avoid AttributeError when property is not a plain dict
        def safe_get(obj: Any, key: str) -> Any:
            if isinstance(obj, dict):
                return obj.get(key)
            return getattr(obj, key, None)

        # Fetch all tables using pagination and cache them
        try:
            tables = self.get_cached_tables(page_size)
            self._cached_tables = tables
        except Exception as e:
            return f"# error: {str(e)}"

        # Process each table
        for t in tables:
            table_name = t.name or ""
            table_id = t.table_id or ""

            # Build Markdown section for this table
            section_lines: list[str] = []
            section_lines.append("---")
            section_lines.append(f"# {table_name}(id:{table_id})")
            section_lines.append("")
            # Simplify table: remove 'Sample' column since sample values are often empty
            section_lines.append("|Field|Type|Extra|")
            section_lines.append("|---|---|---|")

            # Fetch all fields with pagination
            fields = self.get_cached_fields(table_id) or []
            for f in fields:
                fname = f.field_name or ""
                ftype_code = f.type
                prop = f.property or {}
                # Determine a human-readable type label with best-effort heuristics
                base_label = type_map.get(ftype_code)
                rel_table_id_hint = safe_get(prop, "tableId") or safe_get(prop, "table_id")
                options_hint = safe_get(prop, "options")
                if base_label is None:
                    if rel_table_id_hint:
                        # Relation/lookup type without a known mapping
                        ftype = f"关联表({ftype_code})"
                    elif isinstance(options_hint, list):
                        # Select-type field without a known mapping
                        ftype = f"选择({ftype_code})"
                    else:
                        ftype = str(ftype_code)
                else:
                    ftype = base_label

                # Build extra metadata about the field
                extra_parts: List[str] = []

                # If options present (for single/multi select), use first option name as sample
                # Use safe_get because property may be an SDK object rather than a dict
                options = options_hint
                if isinstance(options, list) and options:
                    option_names = [o.get("name") or o.get("text") or "" for o in options if isinstance(o, dict)]
                    if option_names:
                        extra_parts.append("选项：" + "、".join([n for n in option_names if n]))

                # If description present, add to extra
                desc = f.description
                if desc:
                    extra_parts.append(f"说明：{desc}")

                # Some sequence number fields may have prefix/format settings in property
                prefix = safe_get(prop, "prefix") or safe_get(prop, "format_prefix")
                if prefix:
                    extra_parts.append(f"前缀：{prefix}")

                # If relation/lookup properties exist, try to include minimal info
                rel_table_id = rel_table_id_hint
                if rel_table_id:
                    extra_parts.append(f"关联表：{rel_table_id}")

                extra = "；".join(extra_parts) if extra_parts else "无"
                section_lines.append(f"|{fname}|{ftype}|{extra}|")

            markdown_sections.append("\n".join(section_lines))

        return "\n\n".join(markdown_sections)
    
    def handle_list_records(self, page_size: int = 20, page_token: str = None,
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

    def describe_list_records(self, page_size: int = 20, page_token: str = None) -> str:
        """
        Generate Markdown that lists records with expanded (human-readable) field values.
        Always returns per-record JSON sections and uses header style:
        "# Table {table_name} records"

        Args:
            page_size: Number of records per page
            page_token: Token for pagination

        Returns:
            Markdown string containing records in JSON sections
        """
        # Validate required table_id
        if not self.table_id:
            return "# error: table_id is required"

        # Resolve table name for nicer header; fallback to table_id
        table_name = self.table_id
        try:
            tables_meta = self.get_cached_tables(page_size=200)
            if tables_meta and tables_meta.get("success"):
                for t in tables_meta.get("items", []) or []:
                    if t.get("table_id") == self.table_id:
                        tn = t.get("name")
                        if tn:
                            table_name = tn
                            break
        except Exception:
            pass

        # Navigate to requested page using page_token iteration
        page_token = None
        # Fetch the target page
        resp = self.handle_list_records(page_size=page_size, page_token=page_token)
        if not resp.get("success"):
            error_title = resp.get("error", "Failed to list records")
            code = resp.get("code")
            details = [f"table_id: {self.table_id}", f"page_size: {page_size}"]
            if code is not None:
                details.append(f"code: {code}")
            return f"# error: {error_title}\n" + "\n".join(details)

        records = resp.get("records", [])

        # Build Markdown output with requested header style and per-record JSON sections
        lines: List[str] = []
        lines.append(f"# Table {table_name} records")
        lines.append("")
        for r in records:
            rid = r.get("record_id")
            # Normalize field values to JSON-friendly structures
            flat_fields = {k: self._normalize_json_value(v) for k, v in (r.get("fields", {}) or {}).items()}
            lines.append(f"## record_id:{rid}")
            try:
                body = json.dumps(flat_fields, ensure_ascii=False, indent=2)
            except Exception:
                body = str(flat_fields)
            lines.append("```json")
            lines.append(body)
            lines.append("```")
            lines.append("")

        if not records:
            lines.append("")
            lines.append("No records matched the query conditions.")

        return "\n".join(lines)


    def describe_query_records(self, query: Dict[str, Any], 
                              sorts: List[Dict[str, Any]] = None,
                              page_size: int = 20,
                              page_token: str = None) -> str:
        """
        Query records with simplified field-based filtering and return formatted Markdown results.
        
        Args:
            query: Simple query object with field names as keys and values/arrays as values
            sorts: List of sort conditions (optional)
            page_size: Number of records per page (max 100, default 20)
            page_token: Token for pagination (optional)
            
        Returns:
            Markdown string containing the query results
        """
        if not self.table_id:
            return "# error: table_id is required"
        
        try:
            # Convert simple query format to complex filter conditions
            filter_conditions = self._convert_to_filter(query)
            
            # Use the search_records method for advanced querying
            response = self.handle_search_records(
                filter=filter_conditions,
                field_names=None,  # Include all fields
                sorts=sorts,
                page_size=page_size,
                page_token=page_token,
                user_id_type="open_id"  # Use default user_id_type
            )
            
            if not response.success():
                return f"# error: Query failed\n{response.msg}"
            
            # Format the response as Markdown
            data = response.data
            records = data.items or []
            
            # Build Markdown output
            lines = []
            lines.append(f"# Query Results for Table {self.table_id}")
            lines.append("")
            lines.append(f"**Total Records:** {data.total}")
            if data.has_more:
                lines.append(f"**Has More:** Yes (use page_token: {data.page_token})")
            else:
                lines.append("**Has More:** No")
            lines.append("")
            
            if not records:
                lines.append("No records found matching the query conditions.")
            else:
                for record in records:
                    record_id = record.record_id
                    fields = record.fields or {}
                    
                    lines.append(f"## record_id: {record_id}")
                    
                    # Convert fields to JSON format for better readability
                    try:
                        # Normalize field values similar to describe_records
                        normalized_fields = {k: self._normalize_json_value(v) for k, v in fields.items()}
                        json_content = json.dumps(normalized_fields, ensure_ascii=False, indent=2)
                        lines.append("```json")
                        lines.append(json_content)
                        lines.append("```")
                    except Exception:
                        # Fallback to string representation
                        lines.append("```")
                        for field_name, field_value in fields.items():
                            lines.append(f"{field_name}: {field_value}")
                        lines.append("```")
                    
                    lines.append("")
            
            return "\n".join(lines)
            
        except Exception as e:
            return f"# error: Exception occurred during query\n{str(e)}"

    def describe_upsert_record(self, fields: Dict[str, Any]) -> str:
        """
        Upsert a record with enhanced field processing and return Markdown summary.
        
        Logic:
        1. If record_id is provided, use update logic
        2. If no record_id, use auto_number field to match existing record
        3. For related fields, match records in related tables using auto_number field, create if not found
        
        Args:
            fields: Dictionary of field values; may include 'record_id' for direct update
            
        Returns:
            Markdown string describing the upsert result or error
        """
        if not self.table_id:
            return "# error: table_id is required"
        
        try:
            # Process fields and search for existing record
            record_id, processed_data = self._process_fields(fields)
            if record_id:
                return self.describe_update_record(record_id, processed_data)
            else:
                return self.describe_create_record(processed_data)
                
        except Exception as e:
            return f"# error: Failed to upsert record\n{str(e)}\ntable_id: {self.table_id}"

    def _normalize_json_value(self, v):
        """Normalize field values to JSON-friendly structures across methods.
        Intention: Centralize normalization to keep list and single record views consistent.
        """
        # If value is a string that looks like JSON, try to parse it
        if isinstance(v, str):
            s = v.strip()
            if s.startswith("{") or s.startswith("["):
                try:
                    parsed = json.loads(s)
                    return parsed
                except Exception:
                    pass
            return v

        # If value is a dict and carries link-like metadata, return the dict as-is
        if isinstance(v, dict):
            if "table_id" in v or "record_id" in v:
                return v
            # Otherwise, collapse to a readable string using common keys
            ta = v.get("text_arr")
            if isinstance(ta, list) and ta:
                return "、".join([str(x) for x in ta if x is not None])
            for key in ("text", "name", "value"):
                if v.get(key) is not None:
                    return str(v.get(key))
            # Fallback: compact JSON for unknown dict shape
            try:
                return json.dumps(v, ensure_ascii=False, separators=(",", ":"))
            except Exception:
                return str(v)

        # If value is a list, preserve link-like dicts; otherwise normalize to strings
        if isinstance(v, list):
            result = []
            link_like = False
            for item in v:
                # Attempt to parse JSON-looking strings inside the list
                if isinstance(item, str):
                    si = item.strip()
                    if si.startswith("{") or si.startswith("["):
                        try:
                            item = json.loads(si)
                        except Exception:
                            pass
                if isinstance(item, dict) and ("table_id" in item or "record_id" in item):
                    link_like = True
                    result.append(item)
                elif isinstance(item, dict):
                    # Convert non-link dict to a readable string
                    ta = item.get("text_arr")
                    if isinstance(ta, list) and ta:
                        result.append("、".join([str(x) for x in ta if x is not None]))
                    else:
                        for key in ("text", "name", "value"):
                            if item.get(key) is not None:
                                result.append(str(item.get(key)))
                                break
                        else:
                            try:
                                result.append(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
                            except Exception:
                                result.append(str(item))
                else:
                    result.append(str(item))
            if link_like:
                return result
            # When not link-like, join for readability
            parts = [p for p in result if p]
            return "、".join(parts) if parts else ""

        # Fallback for other primitive types
        return str(v) if v is not None else ""

    def describe_query_record(self, record_id: str) -> str:
        """Describe a single record in JSON style with header and normalized fields.
        Intention: Provide consistent per-record JSON output similar to list view.
        """
        if not self.table_id:
            return "# error: table_id is required"

        resp = self.handle_query_record(record_id)
        if not resp.get("success"):
            error_title = resp.get("error", "Failed to query record")
            code = resp.get("code")
            details = [f"table_id: {self.table_id}", f"record_id: {record_id}"]
            if code is not None:
                details.append(f"code: {code}")
            return f"# error: {error_title}\n" + "\n".join(details)

        fields = resp.get("fields", {}) or {}
        flat_fields = {k: self._normalize_json_value(v) for k, v in fields.items()}
        lines: List[str] = []
        lines.append(f"## record_id:{record_id}")
        try:
            body = json.dumps(flat_fields, ensure_ascii=False, indent=2)
        except Exception:
            body = str(flat_fields)
        lines.append("```json")
        lines.append(body)
        lines.append("```")
        return "\n".join(lines)

    def describe_update_record(self, record_id: str, update_fields: Dict[str, Any]) -> str:
        """Update a record and return a Markdown summary in JSON style.
        Intention: Move update logic and formatting out of main.py and keep output consistent.
        """
        if not self.table_id:
            return "# error: table_id is required"
        if not record_id:
            return f"# error: missing record_id\ntable_id: {self.table_id}"
        if not update_fields:
            return f"# error: no fields to update\nrecord_id: {record_id}\ntable_id: {self.table_id}"

        existing = self.handle_query_record(record_id)
        if not existing.get("success"):
            code = existing.get("code")
            details = [f"table_id: {self.table_id}", f"record_id: {record_id}"]
            if code is not None:
                details.append(f"code: {code}")
            return "# error: record not found\n" + "\n".join(details)

        resp = self.handle_update_record(record_id, update_fields)
        if not resp.get("success"):
            error_title = resp.get("error", "Failed to update record")
            code = resp.get("code")
            details = [f"table_id: {self.table_id}", f"record_id: {record_id}"]
            if code is not None:
                details.append(f"code: {code}")
            return f"# error: {error_title}\n" + "\n".join(details)

        # Show only the updated fields, normalized
        flat_fields = {k: self._normalize_json_value(v) for k, v in (update_fields or {}).items()}
        lines: List[str] = []
        lines.append(f"## record_id:{record_id}")
        try:
            body = json.dumps(flat_fields, ensure_ascii=False, indent=2)
        except Exception:
            body = str(flat_fields)
        lines.append("```json")
        lines.append(body)
        lines.append("```")
        return "\n".join(lines)
    
    def describe_create_record(self, fields: Dict[str, Any]) -> str:
        """Create a new record and return a Markdown summary in JSON style.
        Intention: Provide consistent formatting for record creation similar to update_record_markdown.
        """
        if not self.table_id:
            return "# error: table_id is required"
        if not fields:
            return f"# error: no fields to create\ntable_id: {self.table_id}"

        resp = self.handle_create_record(fields)
        # Debug: Log response object type and structure
        if not resp.success():
            logger.error(f"create_record failed - msg: {resp.msg}, code: {resp.code}")
        
        # Handle CreateAppTableRecordResponse object properly
        if not resp.success():
            error_title = resp.msg or "Failed to create record"
            code = resp.code
            details = [f"table_id: {self.table_id}"]
            if code is not None:
                details.append(f"code: {code}")
            return f"# error: {error_title}\n" + "\n".join(details)

        # Extract record information from SDK response
        record = resp.data.record
        record_id = record.record_id
        created_fields = record.fields or {}
        flat_fields = {k: self._normalize_json_value(v) for k, v in created_fields.items()}
        lines: List[str] = []
        lines.append(f"# Created new record")
        lines.append(f"## record_id:{record_id}")
        try:
            body = json.dumps(flat_fields, ensure_ascii=False, indent=2)
        except Exception:
            body = str(flat_fields)
        lines.append("```json")
        lines.append(body)
        lines.append("```")
        return "\n".join(lines)
    
    def find_index_field(self, table_id: str = None) -> Optional[str]:
        """Find the first auto_number type field in the table.
        Intention: Helper method to locate auto-increment fields for upsert operations.
        
        Args:
            table_id: Table ID to search in, defaults to current table
            
        Returns:
            Field name of the first auto_number field, or None if not found
        """
        if table_id is None:
            table_id = self.table_id
            
        fields = self.get_cached_fields(table_id)
        return fields[0].field_name if fields else None
    

    def handle_create_record(self, fields: Dict[str, Any]) -> CreateAppTableRecordResponse:
        """
        Create a new record in a table
        
        Args:
            fields: Dictionary of field values for the new record
            
        Returns:
            CreateAppTableRecordResponse object from the SDK
        """
        # Use provided table_id or fall back to instance table_id
        if not self.table_id:
            raise ValueError("table_id is required either as parameter or instance variable")
            
        # Create record object
        record = AppTableRecord.builder().fields(fields).build()
        request = CreateAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(self.table_id) \
            .request_body(record) \
            .build()
        
        return self.http_client.bitable.v1.app_table_record.create(request)

    def handle_update_record(self, record_id: str, fields: Dict[str, Any]) -> Dict[str, Any]:
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
    
    def handle_delete_record(self, record_id: str) -> Dict[str, Any]:
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
    
    def handle_query_record(self, record_id: str) -> Dict[str, Any]:
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
    
    def handle_search_records(self, 
                      filter: Dict[str, Any],
                      table_id: str = None,
                      field_names: List[str] = None,
                      sorts: List[Dict[str, Any]] = None,
                      page_size: int = 20, page_token: str = None,
                      user_id_type: str = "open_id") -> SearchAppTableRecordResponse:
        """
        Search records in a table with advanced filtering and sorting capabilities
        
        Args:
            filter: Filter conditions object for advanced querying (required)
            field_names: List of field names to include in response (optional)
            sorts: List of sort conditions, each containing 'field_name' and 'desc' (boolean)
            page_size: Number of records per page (max 500, default 20)
            page_token: Token for pagination
            user_id_type: Type of user ID to return ('open_id', 'union_id', 'user_id')
            
        Returns:
            Dictionary containing search results and pagination info
        """
        table_id = table_id or self.table_id
        if not table_id:
            raise ValueError("table_id is required either as parameter or instance variable")
        
        # Build the request
        request_builder = SearchAppTableRecordRequest.builder() \
            .table_id(table_id).app_token(self.app_token) \
            .user_id_type(user_id_type) \
            .page_size(min(page_size, 100))
        if page_token:
            request_builder = request_builder.page_token(page_token)
        
        # Build request body
        body = {}
        if field_names:
            body["field_names"] = field_names
        if sorts:
            # Convert sorts to the expected format
            formatted_sorts = []
            for sort in sorts:
                if isinstance(sort, dict) and "field_name" in sort:
                    formatted_sorts.append({
                        "field_name": sort["field_name"],
                        "desc": sort.get("desc", False)
                    })
            body["sort"] = formatted_sorts
            
        # Add filter condition (required parameter)
        body["filter"] = filter
        request = request_builder.request_body(body).build()
        response = self.http_client.bitable.v1.app_table_record.search(request)
        if response.success():
            return response
        else:
            raise Exception(response.msg)

    def _process_fields(self, fields: Dict[str, Any]) -> Tuple[Optional[str], Dict[str, Any]]:
        """
        Process fields for upsert operation, handling related fields by matching/creating records.
        Also searches for existing records by index field if available.
        
        Args:
            fields: Raw field values
            
        Returns:
            Tuple of (record_id, processed_data)
            - record_id: Record ID (either provided directly or found by index field search)
            - processed_data: Processed field values with related records resolved
        """
        if not fields:
            return None, None
            
        try:
            # Get field metadata to identify related fields
            field_metadata = self.get_cached_fields(self.table_id)
            field_type_map = {f.field_name: f.type for f in field_metadata}
            field_prop_map = {f.field_name: f.property for f in field_metadata}
            
            processed_data = {}
            logger.debug(f"Original fields: {fields}")
            for field_name, field_value in fields.items():
                field_type = field_type_map.get(field_name)
                field_prop = field_prop_map.get(field_name, {})
                if (field_type == 18) and field_value:
                    related_table_id = self._get_related_tid(field_prop)
                    if related_table_id:
                        processed_value = self._get_related_data(
                            field_value, related_table_id
                        )
                        processed_data[field_name] = processed_value
                    else:
                        processed_data[field_name] = field_value
                else:
                    # Non-related field, use as-is
                    processed_data[field_name] = field_value
            logger.debug(f"Processed fields: {processed_data}")
            record_id = processed_data.get("record_id")
            else_data = {k: v for k, v in processed_data.items() if k != "record_id"}
            
            if record_id:
                return record_id, else_data
            # Search for existing record by index field if no direct record_id provided
            index_field = self.find_index_field()
            index_value = else_data.get(index_field)
            if index_field and index_value:
                search_filter = self._convert_to_filter({
                    index_field: index_value,
                })
                logger.debug(f"Search filter: {search_filter}")
                result = self.handle_search_records(search_filter)
                if result.success() and result.data.items:
                    # Found existing record, use its ID as record_id
                    existing_record = result.data.items[0]
                    record_id = existing_record.record_id
                    logger.debug(f"Found existing record: {record_id}")
            return record_id, else_data
        except Exception as e:
            logger.warning(f"Failed to process upsert fields: {str(e)}")
            # Return original fields if processing fails
            return None, fields

    def _get_related_tid(self, property: Dict[str, Any]) -> Optional[str]:
        """
        Extract related table ID from field property.
        
        Args:
            field_property: Field property dictionary
            
        Returns:
            Related table ID or None
        """
        if isinstance(property, dict):
            return property.get("tableId") or property.get("table_id")
        elif hasattr(property, 'tableId'):
            return property.tableId
        elif hasattr(property, 'table_id'):
            return property.table_id
        return None

    def _get_related_data(self, field_value: Any, related_table_id: str) -> Any:
        """
        Process related field value by matching/creating records in the related table.
        
        Args:
            field_value: Value for the related field (can be dict, list, or simple value)
            related_table_id: ID of the related table
            
        Returns:
            Processed field value with record IDs
        """
        try:
            processed_list = []
            if isinstance(field_value, list):
                for item in field_value:
                    processed_item = self._get_related_value(item, related_table_id)
                    if processed_item:
                        processed_list.append(processed_item)
                return processed_list
            else:
                result = self._get_related_value(field_value, related_table_id)
                processed_list.append(result)
                return processed_list
        except Exception as e:
            logger.warning(f"Failed to process related field value: {str(e)}")
            return field_value

    def _get_related_value(self, value: Any, relate_table_id: str) -> str:
        """
        Process a single related record value.
        Args:
            value: Single value (dict with fields or simple value)
            relate_table_id: ID of the related table
        Returns:
            Processed value with record_id
        """
        # If value is already a record reference with record_id, use as-is
        if isinstance(value, dict) and "record_id" in value:
            return value["record_id"]
        
        index_field = self.find_index_field(relate_table_id)
        index_value = value.get(index_field) if isinstance(value, dict) else value
        if index_field and index_value:
            # Search for existing record by auto_number field
            search_filter = self._convert_to_filter({
                index_field: index_value,
            })
            result = self.handle_search_records(search_filter, relate_table_id)
            if result.data.total > 0 and result.data.items:
                return result.data.items[0].record_id
            
            # No existing record found, create new one
            result = self.handle_create_record({index_field: index_value}, relate_table_id)
            if result.success() and result.data:
                record_id = result.data.record.record_id
                logger.debug(f"Created new record, returning: {record_id}")
                return record_id
            
        # Fallback: return original value
        logger.debug(f"Fallback: returning original value: {value}")
        return value

    def _convert_to_filter(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert simple query format to complex filter conditions format.
        
        Args:
            query: Simple query object with field names as keys and values/arrays as values
            
        Returns:
            Complex filter conditions object for search_records
        """
        if not query:
            return {}
        
        conditions = []
        for field_name, field_value in query.items():
            if isinstance(field_value, list):
                # Handle array values - use "is" operator for each value
                for value in field_value:
                    conditions.append({
                        "field_name": field_name,
                        "operator": "is",
                        "value": [value]
                    })
            else:
                # Handle single values - use "is" operator
                conditions.append({
                    "field_name": field_name,
                    "operator": "is", 
                    "value": [field_value]
                })
        
        # Use "and" conjunction to match all conditions
        return {
            "conditions": conditions,
            "conjunction": "and"
        }

