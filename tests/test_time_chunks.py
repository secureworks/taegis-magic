"""Tests for taegis_magic.core.time_chunks — QL time chunking."""

import pytest

from taegis_magic.core.time_chunks import (
    render_time_chunked_queries,
    split_query_pipeline,
)

# ---------------------------------------------------------------------------
# split_query_pipeline
# ---------------------------------------------------------------------------


def test_split_query_pipeline_without_pipeline():
    assert split_query_pipeline("FROM alert WHERE severity >= 0.6") == (
        "FROM alert WHERE severity >= 0.6",
        "",
    )


def test_split_query_pipeline_ignores_quoted_pipe():
    assert split_query_pipeline(
        "FROM alert WHERE @domain MATCHES_REGEX 'a|b' | head 5"
    ) == ("FROM alert WHERE @domain MATCHES_REGEX 'a|b' ", "| head 5")


# ---------------------------------------------------------------------------
# render_time_chunked_queries
# ---------------------------------------------------------------------------


def test_render_time_chunked_queries_appends_placeholders():
    queries = render_time_chunked_queries(
        "FROM alert WHERE severity >= 0.6", "21d", "7d"
    ).split("\n---")

    assert len(queries) == 3
    for query in queries:
        assert query.startswith("FROM alert WHERE severity >= 0.6 EARLIEST='")
        assert " LATEST='" in query


def test_render_time_chunked_queries_keeps_pipeline_last():
    queries = render_time_chunked_queries(
        "FROM process WHERE commandline CONTAINS 'whoami' | head 5", "2d", "1d"
    ).split("\n---")

    assert len(queries) == 2
    for query in queries:
        assert query.endswith("| head 5")


def test_render_time_chunked_queries_uses_template_placeholders():
    queries = render_time_chunked_queries(
        "FROM alert EARLIEST='{{ window.earliest }}' LATEST='{{ window.latest }}'",
        "2d",
        "1d",
    ).split("\n---")

    assert len(queries) == 2
    assert "window.earliest" not in queries[0]


def test_render_time_chunked_queries_removes_explicit_time_range():
    queries = render_time_chunked_queries(
        "FROM alert EARLIEST=-30d LATEST=now", "2d", "1d"
    ).split("\n---")

    assert len(queries) == 2
    for query in queries:
        assert "EARLIEST=-30d" not in query
        assert "LATEST=now" not in query
        assert query.startswith("FROM alert EARLIEST='")
        assert " LATEST='" in query


def test_render_time_chunked_queries_rejects_invalid_duration():
    with pytest.raises(ValueError):
        render_time_chunked_queries("FROM alert", "yesterday", "1d")
