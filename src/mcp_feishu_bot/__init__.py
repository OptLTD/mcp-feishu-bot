"""mcp_feishu_bot package

Purpose: Provide a src-layout package for Feishu MCP server tools and clients.
This enables proper setuptools discovery and clean distribution building.
"""

# Re-export commonly used classes for convenience
from .client import FeishuClient  # noqa: F401
from .message import MessageHandle  # noqa: F401
from .drive import DriveHandle  # noqa: F401
from .bitable import BitableHandle  # noqa: F401