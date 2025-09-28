#!/usr/bin/env python3
"""
Feishu Bitable Operations

Bitable (spreadsheet) operations for Feishu (Lark) API including:
- Create and manage bitables
- Read and write table data
- Manage table structure (fields, views)
"""

import warnings
import json
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

        # Fetch fields with pagination via Feishu Bitable API
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

    def describe_tables_markdown(self, page_size: int = 20) -> str:
        """
        Generate Markdown describing all tables and their fields within the bitable app.
        Always returns a Markdown string. Errors are returned as Markdown with a heading and details.

        Args:
            page_size: Number of tables to return per page (default: 20)

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
            12: "邮件",       # Adjusted: email
            13: "电话",       # Adjusted: phone number
            14: "时间",
            # Best-effort additional mappings based on Feishu Bitable types
            17: "附件",        # Adjusted per user feedback: Attachment type code
            18: "关联表",      # Relation to another table (linked record)
            19: "查找",        # Lookup from related table
            1005: "编号"       # Code/Numbering (adjust if your domain uses a different name)
        }

        markdown_sections: list[str] = []

        # Utility: safely access properties that may be dicts or SDK objects
        # Purpose: Avoid AttributeError when property is not a plain dict
        def safe_get(obj: Any, key: str) -> Any:
            if isinstance(obj, dict):
                return obj.get(key)
            return getattr(obj, key, None)
        page_token = None

        # Iterate through all tables using pagination
        while True:
            tables_resp = self.list_tables(page_size=page_size, page_token=page_token)
            if not tables_resp.get("success"):
                error_title = tables_resp.get("error", "Failed to list tables")
                code = tables_resp.get("code")
                detail_lines: list[str] = []
                if code is not None:
                    detail_lines.append(f"code: {code}")
                return f"# error: {error_title}\n" + ("\n".join(detail_lines) if detail_lines else "")

            tables = tables_resp.get("tables", [])

            for t in tables:
                table_name = t.get("name", "")
                table_id = t.get("table_id", "")

                # Create a new handle bound to this table to list fields
                table_handle = BitableHandle(self.app_token, table_id)

                # Fetch all fields with pagination
                fields: list[Dict[str, Any]] = []
                f_page_token = None
                while True:
                    fields_resp = table_handle.list_fields(page_size=200, page_token=f_page_token)
                    if not fields_resp.get("success"):
                        error_title = fields_resp.get("error", f"Failed to list fields for table {table_id}")
                        code = fields_resp.get("code")
                        detail_lines: list[str] = [f"table_id: {table_id}"]
                        if code is not None:
                            detail_lines.append(f"code: {code}")
                        return f"# error: {error_title}\n" + "\n".join(detail_lines)
                    fields.extend(fields_resp.get("fields", []))
                    if fields_resp.get("has_more"):
                        f_page_token = fields_resp.get("page_token")
                    else:
                        break

                # Build Markdown section for this table
                section_lines: list[str] = []
                section_lines.append("---")
                section_lines.append(f"# {table_name}(id:{table_id})")
                section_lines.append("")
                # Simplify table: remove 'Sample' column since sample values are often empty
                section_lines.append("|Field|Type|Extra|")
                section_lines.append("|---|---|---|")

                for f in fields:
                    fname = f.get("field_name", "")
                    ftype_code = f.get("type")
                    prop = f.get("property") or {}
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
                    desc = f.get("description")
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

            if tables_resp.get("has_more"):
                page_token = tables_resp.get("page_token")
            else:
                break

        return "\n\n".join(markdown_sections)
    
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

    def describe_records_markdown(self, page_size: int = 20, page_index: int = 1, query: Dict[str, Any] | None = None) -> str:
        """
        Generate Markdown that lists records with expanded (human-readable) field values.
        Always returns per-record JSON sections and uses header style:
        "# Table {table_name} records of ({page_index}/{page_size})"

        Args:
            page_size: Number of records per page
            page_index: 1-based index of the page to display
            query: Optional dict of simple equality conditions to filter records (best-effort)

        Returns:
            Markdown string containing records in JSON sections
        """
        # Validate required table_id
        if not self.table_id:
            return "# error: table_id is required"

        # Resolve table name for nicer header; fallback to table_id
        table_name = self.table_id
        try:
            tables_meta = self.list_tables(page_size=200)
            if tables_meta.get("success"):
                for t in tables_meta.get("tables", []) or []:
                    if t.get("table_id") == self.table_id:
                        tn = t.get("name")
                        if tn:
                            table_name = tn
                            break
        except Exception:
            pass

        # Navigate to requested page using page_token iteration
        page_token = None
        total = None
        current_index = 1
        while current_index < page_index:
            resp = self.list_records(page_size=page_size, page_token=page_token)
            if not resp.get("success"):
                error_title = resp.get("error", "Failed to list records")
                code = resp.get("code")
                details = [f"table_id: {self.table_id}", f"page_index: {page_index}", f"page_size: {page_size}"]
                if code is not None:
                    details.append(f"code: {code}")
                return f"# error: {error_title}\n" + "\n".join(details)
            total = resp.get("total", total)
            page_token = resp.get("page_token")
            if not resp.get("has_more"):
                total_pages = (int(total) + page_size - 1) // page_size if total is not None else current_index
                details = [f"table_id: {self.table_id}", f"requested_page_index: {page_index}", f"available_pages: {total_pages}"]
                return "# error: page_index out of range\n" + "\n".join(details)
            current_index += 1

        # Fetch the target page
        resp = self.list_records(page_size=page_size, page_token=page_token)
        if not resp.get("success"):
            error_title = resp.get("error", "Failed to list records")
            code = resp.get("code")
            details = [f"table_id: {self.table_id}", f"page_index: {page_index}", f"page_size: {page_size}"]
            if code is not None:
                details.append(f"code: {code}")
            return f"# error: {error_title}\n" + "\n".join(details)

        records = resp.get("records", [])

        # Apply multi-condition matching from query dict
        def _matches(fields: Dict[str, Any], cond: Dict[str, Any]) -> bool:
            for k, v in cond.items():
                fv = fields.get(k)
                if isinstance(fv, list):
                    # For list fields, check containment by direct equality
                    if v not in fv:
                        return False
                else:
                    if str(fv) != str(v):
                        return False
            return True

        if query:
            records = [r for r in records if _matches(r.get("fields", {}), query or {})]

        # Helper: normalize field value for JSON output
        # Intention: Preserve linked-table structures as JSON objects/lists; otherwise provide readable strings.
        def _normalize_json_value(v):
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

        # Build Markdown output with requested header style and per-record JSON sections
        lines: List[str] = []
        lines.append(f"# Table {table_name} records of ({page_index}/{page_size})")
        lines.append("")
        for r in records:
            rid = r.get("record_id")
            # Normalize field values to JSON-friendly structures
            flat_fields = {k: _normalize_json_value(v) for k, v in (r.get("fields", {}) or {}).items()}
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

    def describe_record_markdown(self, record_id: str) -> str:
        """Describe a single record in JSON style with header and normalized fields.
        Intention: Provide consistent per-record JSON output similar to list view.
        """
        if not self.table_id:
            return "# error: table_id is required"

        resp = self.query_record(record_id)
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

    def update_record_markdown(self, record_id: str, update_fields: Dict[str, Any]) -> str:
        """Update a record and return a Markdown summary in JSON style.
        Intention: Move update logic and formatting out of main.py and keep output consistent.
        """
        if not self.table_id:
            return "# error: table_id is required"
        if not record_id:
            return f"# error: missing record_id\ntable_id: {self.table_id}"
        if not update_fields:
            return f"# error: no fields to update\nrecord_id: {record_id}\ntable_id: {self.table_id}"

        existing = self.query_record(record_id)
        if not existing.get("success"):
            code = existing.get("code")
            details = [f"table_id: {self.table_id}", f"record_id: {record_id}"]
            if code is not None:
                details.append(f"code: {code}")
            return "# error: record not found\n" + "\n".join(details)

        resp = self.update_record(record_id, update_fields)
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
    
