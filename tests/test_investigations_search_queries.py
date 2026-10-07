"""Tests for taegis_magic.commands.utils.investigations.insert_search_query."""

from types import SimpleNamespace

import pandas as pd

from taegis_magic.commands.utils.investigations import (
    insert_search_query,
    list_search_queries,
)


def normalized_results(query_identifier) -> SimpleNamespace:
    """Build a minimal stand-in for a results normalizer."""
    return SimpleNamespace(
        query_identifier=query_identifier,
        tenant_id="tenant1",
        query="FROM alert WHERE severity >= 0.6",
        results_returned=10,
        total_results=30,
    )


def test_insert_search_query_single_id(tmp_path):
    database = str(tmp_path / "test.db")

    insert_search_query(database, normalized_results("query1"))

    df = list_search_queries(database)

    assert list(df["id"]) == ["query1"]


def test_insert_search_query_newline_separated_ids(tmp_path):
    database = str(tmp_path / "test.db")

    insert_search_query(
        database, normalized_results("query1\nquery2\nquery3")
    )

    df = list_search_queries(database)

    assert sorted(df["id"]) == ["query1", "query2", "query3"]
    assert (df["tenant_id"] == "tenant1").all()
    assert (df["results_returned"] == 10).all()
    assert (df["total_results"] == 30).all()
