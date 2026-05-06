import sqlite3
from contextlib import suppress
from textwrap import dedent
from typing import Any, Dict, Hashable, Union

import pandas as pd


def get_notebook_namespace() -> Union[Dict[Hashable, Any], None]:
    """Checks if the program is running within an IPython session
    and, if so, returns the user namespace associated with the
    current session.

    Returns
    -------
    Union[Dict[Hashable, Any], None]
        User namespace from the IPython session
    """
    from IPython.core.getipython import get_ipython

    ip = get_ipython()
    if ip:
        return ip.user_ns
    else:
        return None


def find_dataframe(reference: str) -> pd.DataFrame:
    """Takes a name and attempts to return a pd.DataFrame
    either from a file path on disk or from the notebook
    namespace.

    Parameters
    ----------
    reference : str
        Name referring to the location of a DataFrame

    Returns
    -------
    pd.DataFrame
        DataFrame to handle as investigation evidence

    Raises
    ------
    Exception
        Unable to find a DataFrame with the provided name
    """
    df = None
    notebook_namespace = get_notebook_namespace()

    if notebook_namespace:
        df = notebook_namespace.get(reference)

    if df is None:
        with suppress(FileNotFoundError):
            df = pd.read_json(reference)

    if not isinstance(df, pd.DataFrame):
        raise ValueError(f"Unable to load DataFrame {reference}")

    return df


def get_or_create_database(
    database_uri: str = ":memory:",
) -> sqlite3.Connection:
    """Initializes the database where events, alerts, and search queries
    are staged prior to being added to an investigation.

    Parameters
    ----------
    database_uri : str, optional
        Database filename or URI, by default ":memory:"

    Returns
    -------
    sqlite3.Connection
        Handle to the sqlite database
    """
    db = sqlite3.connect(database_uri)
    with db:
        db.execute(
            dedent(
                """
                CREATE TABLE IF NOT EXISTS investigation_evidence (
                    evidence_type TEXT,
                    id TEXT,
                    tenant_id TEXT,
                    investigation_id TEXT,
                    PRIMARY KEY (id, investigation_id)
                ) WITHOUT ROWID;
                """
            )
        )
        db.execute(
            dedent(
                """
                CREATE TABLE IF NOT EXISTS search_queries (
                    id TEXT,
                    tenant_id TEXT,
                    query TEXT,
                    results_returned INT,
                    total_results INT,
                    inserted_time TEXT,
                    PRIMARY KEY (id)
                ) WITHOUT ROWID;
                """
            )
        )
        db.execute(
            dedent(
                """
                CREATE TABLE IF NOT EXISTS llm_queries (
                    id TEXT,
                    query TEXT,
                    ql TEXT,
                    reason TEXT,
                    types TEXT,
                    status TEXT,
                    inserted_time TEXT,
                    PRIMARY KEY (id)
                ) WITHOUT ROWID;
                """
            )
        )
    return db


def find_database(database_uri: str) -> sqlite3.Connection:
    """Takes a database URI and attempts to connect to the database
    either from a file path on disk or from the notebook namespace.

    Parameters
    ----------
    database_uri : str
        Database filename or URI, by default ":memory:"

    Returns
    -------
    sqlite3.Connection
        Handle to the sqlite database

    Raises
    ------
    Exception
        Could not establish connection to investigation input database
    """
    db = None
    notebook_namespace = get_notebook_namespace()

    if notebook_namespace:
        db = notebook_namespace.get("investigation_input_db")
    elif database_uri == ":memory:":
        raise ValueError(
            "Jupyter namespace not found and database URI is still ':memory:', set URI to a file path."
        )

    if not db:
        db = get_or_create_database(database_uri)

    if not isinstance(db, sqlite3.Connection):
        raise Exception(  # pragma: no cover
            "Could not establish connection to investigation input database"
        )

    if notebook_namespace:
        notebook_namespace["investigation_input_db"] = db

    return db
