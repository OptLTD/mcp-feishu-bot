#!/usr/bin/env python3
"""
Feishu Wiki Operations

Provides search and detail operations for Feishu (Lark) Wiki nodes
and Docs content retrieval.
"""

import warnings
from typing import Dict, Any, Optional

# Suppress deprecation warnings from lark_oapi library
warnings.filterwarnings("ignore", category=DeprecationWarning)

import lark_oapi as lark
from lark_oapi.api.wiki.v1 import (
    SearchNodeRequest,
    SearchNodeRequestBody,
    SearchNodeResponse,
)
from lark_oapi.api.docs.v1 import (
    GetContentRequest,
    GetContentResponse,
)

from .client import FeishuClient
from .utils import to_json_safe
from fastmcp.utilities.logging import get_logger

logger = get_logger(__name__)


class WikiHandle(FeishuClient):
    """
    Feishu Wiki client supporting node search and document detail content.
    """

    def get_content(self, doc_token: str, doc_type: str = "docx", content_type: str = "markdown", lang: str = "zh") -> GetContentResponse:
        """
        Get Feishu Docs content by token.

        Args:
            doc_token: Token of the document
            doc_type: Document type (e.g., docx, doc)
            content_type: Content format (markdown, raw, html)
            lang: Language for content (e.g., zh, en)

        Returns:
            Dict containing raw API data or error info
        """
        request = GetContentRequest.builder() \
            .doc_token(doc_token) \
            .doc_type(doc_type) \
            .content_type(content_type) \
            .lang(lang) \
            .build()

        return self.http_client.docs.v1.content.get(request)

    def describe_get_content(self, doc_token: str, doc_type: str = "docx", content_type: str = "markdown", lang: str = "zh") -> str:
        """
        Fetch document content and render as Markdown.
        """
        resp = self.get_content(doc_token=doc_token, doc_type=doc_type, content_type=content_type, lang=lang)
        if not resp.success():
            return f"# error: Docs content fetch failed\nerror={resp.error}"

        data = to_json_safe(resp.data) or {}
        if content := data.get("content") or "":
            return content
        return "# ok: No content returned"