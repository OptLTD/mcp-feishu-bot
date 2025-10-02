"""
Utility functions for Feishu Bitable operations.

This module contains utility methods extracted from BitableHandle class
to improve code organization and reusability.
"""

import json
import re
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime
from fastmcp.utilities.logging import get_logger

logger = get_logger(__name__)


def normalize_json(v: Any) -> Any:
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
                result.append(item)
        return result if link_like else "、".join([str(x) for x in result if x is not None])

    return v


def parse_datetime(value: Any) -> int:
    """
    Process datetime field value to ensure it's in the correct format.
    Datetime fields must be timestamp in milliseconds or ISO 8601 format.
    
    Args:
        value: Datetime value (can be string, int, or datetime object)
        
    Returns:
        Timestamp in milliseconds
    """
    try:
        # If already a timestamp in milliseconds (13 digits)
        if isinstance(value, int):
            if len(str(value)) == 13:
                return value
            elif len(str(value)) == 10:
                # Convert seconds to milliseconds
                return value * 1000
            else:
                raise ValueError(f"Invalid timestamp format: {value}")
        
        # If it's a string, try to parse it
        if isinstance(value, str):
            # Try ISO 8601 format first
            if re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
                dt = datetime.fromisoformat(value.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)
            
            # Try other common formats
            for fmt in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%Y/%m/%d', '%Y/%m/%d %H:%M:%S']:
                try:
                    dt = datetime.strptime(value, fmt)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    continue
            # Try parsing as timestamp
            try:
                timestamp = int(value)
                if len(str(timestamp)) == 13:
                    return timestamp
                elif len(str(timestamp)) == 10:
                    return timestamp * 1000
            except ValueError:
                pass
        # If it's a datetime object
        if isinstance(value, datetime):
            return int(value.timestamp() * 1000)
        raise ValueError(f"Unable to parse datetime value: {value}")
    except Exception as e:
        logger.warning(f"Failed to process datetime field: {str(e)}")
        # Return original value if processing fails
        return value


def to_json_safe(obj: Any) -> Any:
    """
    Convert SDK objects to JSON-serializable structures safely.

    Rules:
    - Primitives returned as-is
    - Lists/Tuples converted recursively
    - Dicts converted recursively with stringified keys
    - Objects: try `to_dict()` if available; otherwise use `__dict__` recursively; fallback to `str(obj)`
    """
    if obj is None:
        return None
    if isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, (list, tuple)):
        return [to_json_safe(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): to_json_safe(v) for k, v in obj.items()}
    # Try to_dict method
    try:
        to_dict_method = getattr(obj, 'to_dict', None)
        if callable(to_dict_method):
            d = to_dict_method()
            return to_json_safe(d)
    except Exception:
        pass
    # Fallback to __dict__ or str
    try:
        d = getattr(obj, '__dict__', None)
        if isinstance(d, dict):
            return {str(k): to_json_safe(v) for k, v in d.items()}
    except Exception:
        pass
    return str(obj)


def remove_nulls(value: Any) -> Any:
    """
    Recursively remove keys with None values from dicts and filter None items in lists.
    Leaves falsy but meaningful values (e.g. False, 0, "") intact.
    
    Purpose: Clean up field properties by removing null values to reduce output clutter
    """
    if isinstance(value, dict):
        return {k: remove_nulls(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [remove_nulls(v) for v in value if v is not None]
    return value


def query_to_filter(query: Dict[str, Any], field_type_map: Optional[Dict[str, int]] = None) -> Dict[str, Any]:
    """
    Convert simple query format to complex filter conditions format.
    
    Args:
        query: Simple query object with field names as keys and values/arrays as values
        field_type_map: Optional mapping of field names to field types for normalization
        
    Returns:
        Complex filter conditions object for search_records
    """
    if not query:
        return {}
    
    if field_type_map is None:
        field_type_map = {}

    def normalize_value_for_field(name: str, v: Any) -> Any:
        # Handle JSON string cases
        if isinstance(v, str):
            s = v.strip()
            if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
                try:
                    v = json.loads(s)
                except Exception:
                    pass
        # Handle record reference dict, extract record_id
        if isinstance(v, dict) and "record_id" in v:
            v = v.get("record_id")

        t = field_type_map.get(name)
        # Number field: convert string to number
        if t == 2:  # number
            if isinstance(v, str):
                try:
                    if v.isdigit():
                        v = int(v)
                    else:
                        v = float(v)
                except Exception:
                    pass
        # Checkbox/boolean field: convert common strings to boolean
        if t == 6:  # checkbox / boolean
            if isinstance(v, str):
                lv = v.strip().lower()
                if lv in ("true", "1", "yes", "y"): v = True
                elif lv in ("false", "0", "no", "n"): v = False
            elif isinstance(v, (int, float)):
                v = bool(v)
        # Related record field: value should be record ID string
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


def format_field_value(field_name: str, field_value: Any, field_metadata: Dict = None) -> Tuple[str, List[str]]:
    """
    Format field value for display with proper datetime and relation handling.
    
    Args:
        field_name: Name of the field
        field_value: Raw field value
        field_metadata: Field metadata including type information
        
    Returns:
        Tuple of (formatted_value, relation_info_lines)
    """
    relation_lines = []
    
    # Get field type if available
    field_type = None
    if field_metadata:
        field_type = field_metadata.get('type')
    
    # Handle datetime fields (type 5) or fields ending with '时间'
    if (field_type == 5) and isinstance(field_value, (int, str)):
        logger.info(f"Attempting datetime conversion for {field_name}: {field_value}")
        try:
            # Convert timestamp to readable format
            if isinstance(field_value, str) and field_value.isdigit():
                field_value = int(field_value)
            
            if isinstance(field_value, int):
                # Handle both seconds and milliseconds timestamps
                if len(str(field_value)) == 13:  # milliseconds
                    timestamp = field_value / 1000
                elif len(str(field_value)) == 10:  # seconds
                    timestamp = field_value
                else:
                    return str(field_value), relation_lines
                
                dt = datetime.fromtimestamp(timestamp)
                formatted_time = dt.strftime('%Y-%m-%d %H:%M:%S')
                return formatted_time, relation_lines
        except (ValueError, OSError) as e:
            logger.error(f"Failed to convert timestamp for {field_name}: {field_value}, error: {e}")
            pass
    
    # Handle relation fields - show text_arr and collect relation info
    if isinstance(field_value, list):
        text_values = []
        has_relations = False
        for item in field_value:
            if isinstance(item, dict):
                # Extract text_arr for display
                text_arr = item.get('text_arr', [])
                record_ids = item.get('record_ids', [])
                table_id = item.get('table_id', '')

                # If we have record_ids, this is a relation field
                if record_ids:
                    has_relations = True
                    if text_arr:
                        text_values.extend(text_arr)
                        # Collect relation info for separate section
                        for i, record_id in enumerate(record_ids):
                            # Get corresponding text if available
                            text = text_arr[i] if i < len(text_arr) else ''
                            relation_lines.append(f"  - ({record_id}, {text})")
                    else:
                        # Empty text_arr but has record_ids - show record_ids
                        for record_id in record_ids:
                            text_values.append(record_id)
                            relation_lines.append(f"  - ({record_id}, )")
                elif text_arr:
                    # No record_ids but has text_arr - regular text field
                    text_values.extend(text_arr)
                elif table_id:
                    # This is a relation field with table_id but empty text_arr and no record_ids
                    # This indicates an empty relation field - show empty string
                    has_relations = True
                    # Don't add anything to text_values, keep it empty
            else:
                text_values.append(str(item))
        
        if text_values or has_relations:
            # If it's a relation field but text_values is empty, return empty string
            if has_relations and not text_values:
                return '', relation_lines
            return ', '.join(text_values), relation_lines
    
    # Handle single relation field
    if isinstance(field_value, dict):
        text_arr = field_value.get('text_arr', [])
        record_ids = field_value.get('record_ids', [])
        table_id = field_value.get('table_id', '')
        
        if record_ids:
            # This is a relation field
            if text_arr:
                # Has both record_ids and text_arr
                for i, record_id in enumerate(record_ids):
                    # Get corresponding text if available
                    text = text_arr[i] if i < len(text_arr) else ''
                    relation_lines.append(f"  - ({record_id}, {text})")
                return ', '.join(text_arr), relation_lines
            else:
                # Empty text_arr but has record_ids - show record_ids
                for record_id in record_ids:
                    relation_lines.append(f"  - ({record_id}, )")
                return ', '.join(record_ids), relation_lines
        elif text_arr:
            # No record_ids but has text_arr - regular text field
            return ', '.join(text_arr), relation_lines
        elif table_id:
            # This is a relation field with table_id but empty text_arr and no record_ids
            # This indicates an empty relation field - show empty string
            return '', relation_lines
    
    # Default: use normalize_json_value
    return normalize_json(field_value), relation_lines


def format_record(record, field_metadata: Dict = None) -> List[str]:
    """
    Format a single record for display with proper datetime and relation handling.
    
    Args:
        record: Record object with record_id and fields
        field_metadata: Dictionary mapping field names to their metadata
        
    Returns:
        List of formatted lines including relations section
    """
    record_id = getattr(record, 'record_id', '')
    fields = getattr(record, 'fields', {}) or {}
    
    lines = [f"## record_id: {record_id}"]
    relation_dict = {}
    
    for k, v in (fields.items() if isinstance(fields, dict) else []):
        # Get field metadata for this field
        field_meta = field_metadata.get(k, {}) if field_metadata else {}
        formatted_value, relation_info = format_field_value(k, v, field_meta)
        lines.append(f"{k}: {formatted_value}")
        
        # Collect relation info grouped by field name
        if relation_info:
            relation_dict[k] = relation_info
    
    # Add relation section if there are any relations
    if relation_dict:
        lines.append("### relations")
        for field_name, relation_info in relation_dict.items():
            # Get table_id from the record's field data
            field_data = fields.get(field_name, {})
            if isinstance(field_data, dict):
                table_id = field_data.get('table_id', '')
            elif isinstance(field_data, list) and field_data:
                table_id = field_data[0].get('table_id', '') if isinstance(field_data[0], dict) else ''
            lines.append(f"\n{field_name}(table: {table_id}):")
            lines.extend(relation_info)
    return lines