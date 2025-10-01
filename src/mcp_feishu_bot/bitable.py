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
    ListAppTableRecordResponse,
    CreateAppTableRecordResponse,
    SearchAppTableRecordResponse,
    UpdateAppTableRecordResponse,
    DeleteAppTableRecordResponse,
    GetAppTableRecordResponse,
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
        # clear cache data
        self._cached_views = {}
        self._cached_fields = {}
        self._cached_tables = None
        markdown_sections: list[str] = []
        # Fetch all tables using pagination and cache them
        try:
            tables = self.get_cached_tables(page_size)
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
            # fields data type: List[AppTableField]
            fields = self.get_cached_fields(table_id) or []
            for f in fields:
                fname = f.field_name or ""
                # Build type label and concise extra summary via helper
                ftype, extra_summary = self._summarize_field_extra(f)
                section_lines.append(f"|{fname}|{ftype}|{extra_summary}|")

            markdown_sections.append("\n".join(section_lines))

        return "\n\n".join(markdown_sections)

    def _summarize_field_extra(self, field: AppTableField) -> Tuple[str, str]:
        """
        Generate a human-readable type label and concise extra summary.
        Only keeps key info per type to avoid bloated output.
        """
        # Safe get for dict/SDK objects
        def sg(obj: Any, key: str) -> Any:
            if isinstance(obj, dict):
                return obj.get(key)
            return getattr(obj, key, None)

        code = getattr(field, 'type', None)
        prop = getattr(field, 'property', {}) or {}

        # Determine a human-readable type label with best-effort heuristics
        type_map = {
            1: "文本",
            2: "数字",
            3: "单选",
            4: "多选",
            5: "日期",
            7: "复选框",
            11: "人员",
            13: "电话号码",
            15: "超链接",
            17: "附件",
            18: "单项关联",
            19: "查找",
            20: "公式（不支持设置公式表达式）",
            21: "双向关联",
            22: "地理位置",
            23: "群组",
            1001: "创建时间",
            1002: "最后更新时间",
            1003: "创建人",
            1004: "修改人",
            1005: "自动编号",
        }
        base_label = type_map.get(code)
        rel_table_id_hint = sg(prop, "tableId") or sg(prop, "table_id")
        options_hint = sg(prop, "options")
        if base_label is None:
            if rel_table_id_hint:
                # Relation/lookup type without a known mapping
                ftype = f"关联表({code})"
            elif isinstance(options_hint, list):
                # Select-type field without a known mapping
                ftype = f"选择({code})"
            else:
                ftype = str(code)
        else:
            ftype = base_label

        # Single/Multi Select
        if code in (3, 4):
            options = sg(prop, 'options')
            if isinstance(options, list) and options:
                names: List[str] = []
                for o in options:
                    if isinstance(o, dict):
                        name = o.get('name') or o.get('text')
                    else:
                        name = sg(o, 'name') or sg(o, 'text')
                    if name:
                        names.append(name)
                if names:
                    more = ''
                    if len(names) > 5:
                        names = names[:5]
                        more = '等'
                    return ftype, '选项：' + '、'.join(names) + more

        # Date/DateTime
        if code == 5:
            fmt = sg(prop, 'date_formatter')
            auto = sg(prop, 'auto_fill')
            parts: List[str] = []
            if fmt:
                parts.append(f'日期格式：{fmt}')
            if auto is True:
                parts.append('自动填充：是')
            return ftype, ('；'.join(parts) if parts else '无')

        # 数字/金额
        if code == 2:
            fmt = sg(prop, 'formatter')
            cur = sg(prop, 'currency_code')
            parts: List[str] = []
            if cur:
                parts.append(f'币种：{cur}')
            if fmt:
                parts.append(f'格式：{fmt}')
            return ftype, ('；'.join(parts) if parts else '无')

        # 自动编号
        if code in (10, 1005):
            prefix = sg(prop, 'prefix') or sg(prop, 'format_prefix')
            return ftype, (f'前缀：{prefix}' if prefix else '无')

        # 关联（单项/双向）
        if code in (18, 21):
            table_name = sg(prop, 'table_name') or sg(prop, 'tableName')
            table_id = sg(prop, 'table_id') or sg(prop, 'tableId')
            multiple = sg(prop, 'multiple')
            base = table_name or table_id
            parts: List[str] = []
            if base:
                parts.append(f'关联表：{base}')
            if isinstance(multiple, bool):
                parts.append(f'多选：{"是" if multiple else "否"}')
            return ftype, ('；'.join(parts) if parts else '无')

        # 查找（保留 19 兼容）
        if code == 19:
            target_field = sg(prop, 'target_field')
            if target_field:
                return ftype, f'目标字段：{target_field}'
            return ftype, '查找'

        # Default: use description if available, otherwise '无'
        desc = getattr(field, 'description', None)
        return ftype, (f'说明：{desc}' if desc else '无')
    
    def handle_list_records(self, page_size: int = 20, page_token: str = None,
                    view_id: str = None, filter_condition: str = None,
                    sort: List[str] = None) -> ListAppTableRecordResponse:
        """
        List records in a table
        
        Args:
            page_size: Number of records to return per page
            page_token: Token for pagination
            view_id: ID of the view to use
            filter_condition: Filter condition for records
            sort: List of sort conditions
            
        Returns:
            Raw SDK response object
        """
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

        return self.http_client.bitable.v1.app_table_record.list(request)

    def describe_list_records(self, page_size: int = 20, page_token: str = None) -> str:
        """
        列出记录并返回 Markdown 文本。保留分页信息与错误详情。
        """
        if not self.table_id:
            return "# error: table_id is required"

        try:
            resp = self.handle_list_records(page_size=page_size, page_token=page_token)
        except Exception as e:
            return f"# error: {str(e)}\npage_size: {page_size}"

        if not resp.success():
            # 尝试提取 SDK 错误详情
            msg = getattr(resp, 'msg', None)
            error = getattr(resp, 'error', None)
            return f"# error: {msg}:\n{error}"

        items = getattr(resp.data, 'items', []) if getattr(resp, 'data', None) else []
        lines = ["# Records", ""]
        for rec in items or []:
            record_id = getattr(rec, 'record_id', '')
            fields = getattr(rec, 'fields', {}) or {}
            lines.append(f"## record_id: {record_id}")
            for k, v in (fields.items() if isinstance(fields, dict) else []):
                lines.append(f"- {k}: {self._normalize_json_value(v)}")
            lines.append("")

        # 分页信息
        if getattr(resp.data, 'has_more', False):
            lines.append(f"has_more: {resp.data.has_more}")
            if getattr(resp.data, 'page_token', None):
                lines.append(f"next_page_token: {resp.data.page_token}")

        return "\n".join(lines)


    def describe_search_records(self, query: Dict[str, Any], 
                              sorts: List[Dict[str, Any]] = None,
                              page_size: int = 20,
                              page_token: str = None) -> str:
        """
        搜索记录并返回 Markdown 文本。支持简化查询、排序与分页，保留错误详情。
        """
        if not self.table_id:
            return "# error: table_id is required"
        if not query or len(query) == 0:
            return "# error: query cannot empty"

        filters = self._convert_to_filter(query or {})
        try:
            resp = self.handle_search_records(
                filter=filters, page_size=page_size, page_token=page_token,
                field_names=None, sorts=sorts, user_id_type="open_id"
            )
        except Exception as e:
            return f"# error: {str(e)}\nquery: {json.dumps(query, ensure_ascii=False)}"

        if not resp.success():
            # 尝试提取 SDK 错误详情
            msg = getattr(resp, 'msg', None)
            error = getattr(resp, 'error', None)
            return f"# error: {msg}:\n{error}\nquery: {json.dumps(query, ensure_ascii=False)}"

        items = getattr(resp.data, 'items', []) if getattr(resp, 'data', None) else []
        lines = ["# Search Results", ""]
        for rec in items or []:
            record_id = getattr(rec, 'record_id', '')
            fields = getattr(rec, 'fields', {}) or {}
            lines.append(f"## record_id: {record_id}")
            for k, v in (fields.items() if isinstance(fields, dict) else []):
                lines.append(f"- {k}: {self._normalize_json_value(v)}")
            lines.append("")

        # 分页信息
        if getattr(resp.data, 'has_more', False):
            lines.append(f"has_more: {resp.data.has_more}")
            if getattr(resp.data, 'page_token', None):
                lines.append(f"next_page_token: {resp.data.page_token}")

        return "\n".join(lines)

    def describe_upsert_record(self, fields: Dict[str, Any]) -> str:
        """
        Upsert 记录并返回 Markdown 文本。根据是否存在 record_id 或索引字段决定更新或创建。
        """
        if not self.table_id:
            return "# error: table_id is required"
        if not fields:
            return "# error: fields is required"

        # 预处理字段（解析关联表、索引匹配等）
        record_id, processed = self._process_fields(fields)
        try:
            if record_id:
                resp = self.handle_update_record(record_id, processed)
                action = "update"
            else:
                resp = self.handle_create_record(processed)
                action = "create"
        except Exception as e:
            return f"# error: {str(e)}"

        if not resp.success():
            # 尝试提取 SDK 错误详情
            msg = getattr(resp, 'msg', None)
            error = getattr(resp, 'error', None)
            return f"# error: {msg}:\n{error}"

        rid = record_id
        if not rid:
            rec = getattr(getattr(resp, 'data', None), 'record', None)
            rid = getattr(rec, 'record_id', '') if rec else ''

        lines = [f"# {action}d record_id: {rid}", ""]
        for k, v in (processed.items() if isinstance(processed, dict) else []):
            lines.append(f"- {k}: {self._normalize_json_value(v)}")
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

    def describe_query_record(self, record_id: str) -> str:
        """
        获取单条记录并返回 Markdown 文本，包含字段详情与错误信息。
        """
        if not self.table_id:
            return "# error: table_id is required"
        if not record_id:
            return "# error: record_id is required"

        try:
            resp = self.handle_query_record(record_id)
        except Exception as e:
            return f"# error: {str(e)}\nrecord_id: {record_id}"

        if not resp.success():
            # 尝试提取 SDK 错误详情
            msg = getattr(resp, 'msg', None)
            error = getattr(resp, 'error', None)
            return f"# error: {msg}:\n{error}\nrecord_id: {record_id}"

        rec = getattr(getattr(resp, 'data', None), 'record', None)
        if not rec:
            return f"# Not found\nrecord_id: {record_id}"

        lines = [f"# record_id: {getattr(rec, 'record_id', '')}", ""]
        fields = getattr(rec, 'fields', {}) or {}
        for k, v in (fields.items() if isinstance(fields, dict) else []):
            lines.append(f"- {k}: {self._normalize_json_value(v)}")
        return "\n".join(lines)

    def describe_update_record(self, record_id: str, update_fields: Dict[str, Any]) -> str:
        """
        更新记录并返回 Markdown 文本，包含更新字段与错误详情。
        """
        if not self.table_id:
            return "# error: table_id is required"
        if not record_id:
            return "# error: record_id is required"
        if not update_fields:
            return "# error: no fields to update"

        try:
            resp = self.handle_update_record(record_id, update_fields)
        except Exception as e:
            return f"# error: {str(e)}\nrecord_id: {record_id}"

        if not resp.success():
            # 尝试提取 SDK 错误详情
            msg = getattr(resp, 'msg', None)
            error = getattr(resp, 'error', None)
            return f"# error: {msg}:\n{error}\nrecord_id: {record_id}"

        lines = [f"# Updated record_id: {record_id}", ""]
        for k, v in update_fields.items():
            lines.append(f"- {k}: {self._normalize_json_value(v)}")
        return "\n".join(lines)
    
    def describe_create_record(self, fields: Dict[str, Any]) -> str:
        """
        创建记录并返回 Markdown 文本，包含字段与错误详情。
        """
        if not self.table_id:
            return "# error: table_id is required"
        if not fields:
            return "# error: no fields to create"

        try:
            resp = self.handle_create_record(fields)
        except Exception as e:
            return f"# error: {str(e)}"

        if not resp.success():
            # 尝试提取 SDK 错误详情
            msg = getattr(resp, 'msg', None)
            error = getattr(resp, 'error', None)
            return f"# error: {msg}:\n{error}"

        record = getattr(getattr(resp, 'data', None), 'record', None)
        rid = getattr(record, 'record_id', '') if record else ''
        lines = [f"# Created record_id: {rid}", ""]
        for k, v in fields.items():
            lines.append(f"- {k}: {self._normalize_json_value(v)}")
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

    def handle_update_record(self, record_id: str, fields: Dict[str, Any]) -> UpdateAppTableRecordResponse:
        """
        Update an existing record in a table
        
        Args:
            record_id: The ID of the record to update
            fields: Dictionary of field values to update
            table_id: The ID of the table (optional, uses instance table_id if not provided)
            
        Returns:
            UpdateAppTableRecordResponse object from the SDK
        """
        if not self.table_id:
            raise ValueError("table_id is required either as parameter or instance variable")
        # Create record object with updated fields
        record = AppTableRecord.builder().fields(fields).build()
        request = UpdateAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(self.table_id) \
            .record_id(record_id) \
            .request_body(record) \
            .build()
        return self.http_client.bitable.v1.app_table_record.update(request)
    
    def handle_delete_record(self, record_id: str) -> DeleteAppTableRecordResponse:
        """
        Delete a record from a table
        
        Args:
            record_id: The ID of the record to delete
            table_id: The ID of the table (optional, uses instance table_id if not provided)
            
        Returns:
            DeleteAppTableRecordResponse object from the SDK
        """
        if not self.table_id:
            raise ValueError("table_id is required either as parameter or instance variable")
        request = DeleteAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(self.table_id) \
            .record_id(record_id) \
            .build()
        return self.http_client.bitable.v1.app_table_record.delete(request)
    
    def handle_query_record(self, record_id: str) -> GetAppTableRecordResponse:
        """
        Get a specific record from a table
        
        Args:
            record_id: The ID of the record to retrieve
            
        Returns:
            GetAppTableRecordResponse object from the SDK
        """
        if not self.table_id:
            raise ValueError("table_id is required either as parameter or instance variable")
        request = GetAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(self.table_id) \
            .record_id(record_id) \
            .build()
        return self.http_client.bitable.v1.app_table_record.get(request)
    
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
        return self.http_client.bitable.v1.app_table_record.search(request)

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
        
        # 获取字段类型映射，便于按类型归一化值
        try:
            field_metadata = self.get_cached_fields(self.table_id)
        except Exception:
            field_metadata = None
        field_type_map = {}
        if field_metadata:
            try:
                field_type_map = {getattr(f, 'field_name', None): getattr(f, 'type', None) for f in field_metadata}
            except Exception:
                field_type_map = {}

        def normalize_value_for_field(name: str, v: Any) -> Any:
            # 处理字符串型 JSON 的情况
            if isinstance(v, str):
                s = v.strip()
                if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                    try:
                        v = json.loads(s)
                    except Exception:
                        pass
            # 处理记录引用字典，提取 record_id
            if isinstance(v, dict) and "record_id" in v:
                v = v.get("record_id")

            t = field_type_map.get(name)
            # 数值字段：将字符串转为数字
            if t == 2:  # number
                if isinstance(v, str):
                    try:
                        if v.isdigit():
                            v = int(v)
                        else:
                            v = float(v)
                    except Exception:
                        pass
            # 复选框/布尔字段：将常见字符串转为布尔
            if t == 6:  # checkbox / boolean
                if isinstance(v, str):
                    lv = v.strip().lower()
                    if lv in ("true", "1", "yes", "y"): v = True
                    elif lv in ("false", "0", "no", "n"): v = False
                elif isinstance(v, (int, float)):
                    v = bool(v)
            # 关联记录字段：值应为记录ID字符串
            if t == 18:  # relation
                if isinstance(v, dict) and "record_id" in v:
                    v = v.get("record_id")
            return v

        conditions = []
        for field_name, field_value in query.items():
            if isinstance(field_value, list):
                for value in field_value:
                    nv = normalize_value_for_field(field_name, value)
                    conditions.append({
                        "field_name": field_name,
                        "operator": "is",
                        "value": [nv]
                    })
            else:
                nv = normalize_value_for_field(field_name, field_value)
                conditions.append({
                    "field_name": field_name,
                    "operator": "is",
                    "value": [nv]
                })
        
        # Use "and" conjunction to match all conditions
        return {
            "conditions": conditions,
            "conjunction": "and"
        }

