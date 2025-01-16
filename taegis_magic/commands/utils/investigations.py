"""Taegis Investigation utilities."""

import logging
import operator
import sqlite3
from contextlib import suppress
from dataclasses import asdict, dataclass, field
from enum import Enum
from functools import reduce
from textwrap import dedent
from typing import Any, Dict, Hashable, List, Optional, Union

import pandas as pd
from dataclasses_json import dataclass_json

from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.utils import get_tenant_id_column
from taegis_magic.core.graphql.subjects import lookup_federated_subject
from taegis_sdk_python import GraphQLService

log = logging.getLogger(__name__)


class InvestigationEvidenceType(str, Enum):
    """Taegis Investigations Evidence Types."""

    Alert = "alerts"
    Event = "events"
    Query = "search_queries"


@dataclass_json
@dataclass
class InvestigationEvidenceChanges:
    """Taegis Investigation Evidence Changes."""

    action: str
    evidence_type: InvestigationEvidenceType
    investigation_id: str
    before: int = 0
    after: int = 0
    difference: int = 0


@dataclass_json
@dataclass
class InvestigationEvidence:
    """Taegis Investigation Evidence."""

    tenant_id: str
    investigation_id: str
    alerts: Optional[List[str]] = None
    events: Optional[List[str]] = None
    search_queries: Optional[List[str]] = None


@dataclass_json
@dataclass
class InvestigationEvidenceNormalizer(TaegisResultsNormalizer):
    """Taegis Investigation Evidence Normalizer."""

    raw_results: InvestigationEvidenceChanges = field(
        default_factory=lambda: InvestigationEvidenceChanges(
            action="",
            evidence_type=InvestigationEvidenceType.Alert,
            investigation_id="",
        )
    )

    @property
    def results(self):
        return [asdict(self.raw_results)]

    def _repr_markdown_(self):
        """Represent as markdown."""
        return dedent(
            f"""
            **Investigation ID**: {self.raw_results.investigation_id}

            | Action | Evidence Type | Staged Before Change | Staged After Change | Difference |
            | ------ | ------------- | -------------------- | ------------------- | ---------- |
            | {self.raw_results.action} | {self.raw_results.evidence_type} | {self.raw_results.before} | {self.raw_results.after} | {self.raw_results.difference} |
            """
        )


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
    return db


def stage_investigation_evidence(
    df: pd.DataFrame,
    db: sqlite3.Connection,
    evidence_type: InvestigationEvidenceType,
    investigation_id: str = "NEW",
):
    """Takes a pd.DataFrame containing Taegis resource names and
    appends them to a given table in the sqlite database to stage
    for adding to an investigation.

    This works by saving the DataFrame as a temporary table in
    the database, then doing an insert into the appropriate table
    that silently drops rows that violate the uniqueness constraint.

    Parameters
    ----------
    df : pd.DataFrame
        pd.DataFrame containing Taegis resource names
    db : sqlite3.Connection
        Handle to the sqlite database
    evidence_type : InvestigationEvidenceType
        Kind of evidence to add to investigation, used to identify proper database tables
    investigation_id : str, optional
        Taegis investigation ID, by default "NEW"
    """

    before_changes = read_database(
        db, evidence_type=evidence_type, investigation_id=investigation_id
    )

    tenant_column = get_tenant_id_column(df)

    if (
        evidence_type == InvestigationEvidenceType.Event
        or evidence_type == InvestigationEvidenceType.Event.value
    ):
        df = df.assign(id=df["resource_id"])

    df[["id", tenant_column]].assign(
        investigation_id=investigation_id,
        evidence_type=evidence_type,
    )[["evidence_type", "id", tenant_column, "investigation_id"]].to_sql(
        "temp_table",
        con=db,
        index=False,
        if_exists="replace",
    )
    with db:
        cur = db.cursor()
        cur.execute(
            "INSERT or IGNORE INTO investigation_evidence SELECT * FROM temp_table"
        )

    after_changes = read_database(
        db, evidence_type=evidence_type, investigation_id=investigation_id
    )

    return InvestigationEvidenceChanges(
        action="stage",
        evidence_type=evidence_type,
        investigation_id=investigation_id,
        before=len(before_changes),
        after=len(after_changes),
        difference=(len(after_changes) - len(before_changes)),
    )


def unstage_investigation_evidence(
    df: pd.DataFrame,
    db: sqlite3.Connection,
    evidence_type: InvestigationEvidenceType,
    investigation_id: str = "NEW",
):
    """Takes a pd.DataFrame containing Taegis resource names and
    removes any rows from the provided table in the database that
    match those resource names and the same investigation ID.

    This works by reading in the database table into a DataFrame,
    filtering out the desired resource names, dropping the old table
    rows, and overwriting the table with the newly filtered rows.

    Parameters
    ----------
    df : pd.DataFrame
        pd.DataFrame containing Taegis resource names
    db : sqlite3.Connection
        Handle to the sqlite database
    evidence_type : InvestigationEvidenceType
        Kind of evidence to add to investigation, used to identify proper database tables
    investigation_id : str, optional
        Taegis investigation ID, by default "NEW"
    """
    tenant_column = get_tenant_id_column(df)

    before_changes = read_database(
        db, evidence_type=evidence_type, investigation_id=investigation_id
    )

    staged_evidence = read_database(db)
    remaining_evidence = staged_evidence[
        ~(
            (staged_evidence["id"].isin(df["id"].unique()))
            & (staged_evidence["evidence_type"] == evidence_type)
            & (staged_evidence["investigation_id"] == investigation_id)
        )
    ]
    remaining_evidence[
        ["evidence_type", "id", tenant_column, "investigation_id"]
    ].to_sql(
        "temp_table",
        con=db,
        index=False,
        if_exists="replace",
    )
    with db:
        cur = db.cursor()
        cur.execute("DELETE FROM investigation_evidence")
        cur.execute(
            "INSERT or IGNORE INTO investigation_evidence SELECT * FROM temp_table"
        )

    after_changes = read_database(
        db, evidence_type=evidence_type, investigation_id=investigation_id
    )

    return InvestigationEvidenceChanges(
        action="unstage",
        evidence_type=evidence_type,
        investigation_id=investigation_id,
        before=len(before_changes),
        after=len(after_changes),
        difference=(len(after_changes) - len(before_changes)),
    )


def get_investigation_evidence(
    database_uri: str, tenant_id: str, investigation_id: str = "NEW"
) -> InvestigationEvidence:
    """Reads the investigation input database and returns Taegis
    resource identifiers for the specific tenant and investigation
    of interest.

    Parameters
    ----------
    database_uri : str
        URI to the investigation input database
    tenant_id : str
        Taegis tenant ID associated with the investigation
    investigation_id : str, optional
        Filters evidence to the specific investigation ID, by default "NEW"

    Returns
    -------
    InvestigationEvidence
        Dataclass containing alert, event, and search query identifiers
    """

    db = find_database(database_uri)

    alerts = (
        read_database(
            db,
            evidence_type=InvestigationEvidenceType.Alert,
            tenant_id=tenant_id,
            investigation_id=investigation_id,
        )["id"]
        .unique()
        .tolist()
    )

    events = (
        read_database(
            db,
            evidence_type=InvestigationEvidenceType.Event,
            tenant_id=tenant_id,
            investigation_id=investigation_id,
        )["id"]
        .unique()
        .tolist()
    )

    search_queries = (
        read_database(
            db,
            evidence_type=InvestigationEvidenceType.Query,
            tenant_id=tenant_id,
            investigation_id=investigation_id,
        )["id"]
        .unique()
        .tolist()
    )

    # Do any special handling here, such as "saving" search queries...

    return InvestigationEvidence(
        tenant_id=tenant_id,
        investigation_id=investigation_id,
        alerts=alerts or None,
        events=events or None,
        search_queries=search_queries or None,
    )


def read_database(
    db: sqlite3.Connection,
    evidence_type: Optional[InvestigationEvidenceType] = None,
    tenant_id: Optional[str] = None,
    investigation_id: Optional[str] = None,
) -> pd.DataFrame:
    """Reads a specific table in the database and
    returns as a pd.DataFrame.

    Parameters
    ----------
    db : sqlite3.Connection
        Handle to the sqlite database
    evidence_type : InvestigationEvidenceType, optional
       Filters evidence to the specific evidence type
    tenant_id : str, optional
        Filters evidence to the specific Taegis tenant ID
    investigation_id : str, optional
        Filters evidence to the specific investigation ID

    Returns
    -------
    pd.DataFrame
        DataFrame representation of the rows in the table
    """
    df = pd.read_sql("SELECT * FROM investigation_evidence", con=db)

    filters = []

    if evidence_type:
        filters.append(df.evidence_type == evidence_type)
    if tenant_id:
        filters.append(df.tenant_id == tenant_id)
    if investigation_id:
        filters.append(df.investigation_id == investigation_id)

    if filters:
        return df[reduce(operator.and_, filters)].copy()
    else:
        return df.copy()


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


def insert_search_query(database_uri: str, normalized_results):
    """Insert a Taegis search query."""
    db = find_database(database_uri)

    with db:
        db.execute(
            """
        INSERT INTO search_queries VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
        """,
            (
                normalized_results.query_identifier,
                normalized_results.tenant_id,
                normalized_results.query,
                normalized_results.results_returned,
                normalized_results.total_results,
            ),
        )


def list_search_queries(database_uri: str):
    """List Taegis Search Queries."""
    db = find_database(database_uri)

    df = pd.read_sql("SELECT * FROM search_queries", con=db)

    return df


def delete_search_query(database_uri: str, query_id: str):
    """Insert a Taegis search query."""
    db = find_database(database_uri)

    with db:
        db.execute(
            """
        DELETE FROM search_queries WHERE id = ?
        """,
            (query_id,),
        )


def clear_search_queries(database_uri: str):
    """Clear stored search queries"""
    db = find_database(database_uri)

    with db:
        db.execute(
            """
        DELETE FROM search_queries
        """
        )


def lookup_assignee_id(service: GraphQLService, assignee_id: str) -> str:
    """Lookup and format assignee ID for Taegis Investigations.

    Parameters
    ----------
    service : GraphQLService
        Taegis SDK for Python GraphQLService object.
    assignee_id : str
        Assignee ID to lookup.

    Returns
    -------
    str
        Formatted Assignee ID.

    Raises
    ------
    ValueError
    """
    if assignee_id == "@me":
        log.debug("Looking up current subject...")
        subject = lookup_federated_subject(service)
        assignee_id = subject.get("id")

        if not assignee_id:
            raise ValueError(f"Could not determine Subject ID: {subject}")

        if subject.get("identity", {}).get("__typename") == "Client":
            log.debug("Subject is client.  Updating assignee_id with `@clients`...")
            assignee_id += "@clients"

    elif assignee_id == "@partner":
        log.debug("Looking up partner mention in preferences...")
        preferences = service.preferences.query.partner_preferences()

        if not preferences.mention:
            raise ValueError(f"Could not determine Partner Mention: {preferences}")

        assignee_id = f"@{preferences.mention}"

    # alias to keep the partner/organization/tenant language consistent
    elif assignee_id == "@tenant":
        assignee_id = "@customer"

    elif (
        "@" in assignee_id  # probably an email
        and not assignee_id.startswith("@")  # don't lookup submitted mentions
        and not assignee_id.endswith("@clients")  # don't lookup submitted clients
    ):
        log.debug("Looking up user {assignee_id} by email...")

        # search for email in subject accessible tenants
        subject = service.subjects.query.current_subject()
        users = []
        for tenant_id in subject.role_assignment_data.assigned_tenant_ids:
            log.debug("Looking up user {assignee_id} in {tenant_id}...")
            with service(tenant_id=tenant_id):
                users = service.users.query.tdrusers(email=assignee_id)
                if users:
                    break

        # search for email in tenant context
        if not users:
            log.debug("Looking up user {assignee_id} in {service.tenant_id}...")
            users = service.users.query.tdrusers(email=assignee_id)

        if users:
            log.debug("User {assignee_id} found. Using ID: {users[0].id}")
            assignee_id = users[0].id

            if not assignee_id:
                raise ValueError(f"Could not determine User ID: {users}")
        else:
            log.warning(f"User {assignee_id} not found.  Using ID: {assignee_id}...")

    return assignee_id
