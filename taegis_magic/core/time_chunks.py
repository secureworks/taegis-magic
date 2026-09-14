"""Taegis Magic QL time chunking."""

import logging
import re
from typing import Tuple

from taegis_sdk_python.templates import load_jinja2_template_environment, time_split

log = logging.getLogger(__name__)

TIME_WINDOW_PLACEHOLDERS = (
    "EARLIEST='{{ window.earliest }}' LATEST='{{ window.latest }}'"
)


def split_query_pipeline(cell: str) -> Tuple[str, str]:
    """Split a query into the search expression and the trailing pipeline."""
    in_quotes = False
    index = 0

    while index < len(cell):
        character = cell[index]

        if in_quotes:
            if character == "\\":
                index += 2
                continue
            if character == "'":
                in_quotes = False
        elif character == "'":
            in_quotes = True
        elif character == "|":
            return cell[:index], cell[index:]

        index += 1

    return cell, ""


def render_time_chunked_queries(cell: str, time_window: str, time_chunk: str) -> str:
    """Render a query as `---` delimited queries, one per time chunk."""
    if "{{ window.earliest }}" not in cell and "{{ window.latest }}" not in cell:
        expression, pipeline = split_query_pipeline(cell)

        expression = re.sub(
            r"\b(?:earliest|latest)\s*=\s*(?:\"[^\"]*\"|'[^']*'|\S+)",
            "",
            expression,
            flags=re.IGNORECASE,
        )
        expression = re.sub(r"\s+", " ", expression).strip()

        cell = " ".join(
            part
            for part in (expression.strip(), TIME_WINDOW_PLACEHOLDERS, pipeline.strip())
            if part
        )

    return time_split(
        environment=load_jinja2_template_environment(),
        template_text=cell,
        initial=time_window,
        chunk=time_chunk,
    )
