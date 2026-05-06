"""SQLite utilities for NLSearchOutputsV2."""

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
from taegis_magic.commands.utils._database import find_database

from taegis_sdk_python.services.llm_service.types import NLSearchOutputV2


def read_database(
    db: sqlite3.Connection,
) -> pd.DataFrame:
    """Reads a specific table in the database and
    returns as a pd.DataFrame.

    Parameters
    ----------
    db : sqlite3.Connection
        Handle to the sqlite database

    Returns
    -------
    pd.DataFrame
        DataFrame representation of the rows in the table
    """
    db = find_database(database_uri)

    df = pd.read_sql("SELECT * FROM llm_queries", con=db)

    return df.copy()


def insert_nl_search_query(database_uri: str, query: str, results: NLSearchOutputV2):
    """Insert a Taegis search query."""
    db = find_database(database_uri)

    with db:
        db.execute(
            """
        INSERT INTO llm_queries VALUES (?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
        """,
            (
                results.id,
                query,
                results.ql,
                results.reason,
                ",".join(results.types or []),
                results.status,
            ),
        )


def list_nl_search_queries(database_uri: str):
    """List Taegis NL Search Queries."""
    db = find_database(database_uri)

    df = pd.read_sql("SELECT * FROM llm_queries", con=db)

    return df


def delete_nl_search_query(database_uri: str, query_id: str):
    """Delete a Taegis NL search query."""
    db = find_database(database_uri)

    with db:
        db.execute(
            """
        DELETE FROM llm_queries WHERE id = ?
        """,
            (query_id,),
        )


def clear_nl_search_queries(database_uri: str):
    """Clear stored NL search queries"""
    db = find_database(database_uri)

    with db:
        db.execute(
            """
        DELETE FROM llm_queries
        """
        )
