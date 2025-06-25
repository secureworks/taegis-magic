"""Pandas functions for investigations."""

import logging
from dataclasses import asdict

import pandas as pd
from taegis_magic.commands.utils.investigations import InvestigationEvidenceType
from taegis_magic.core.service import get_service
from taegis_magic.core.utils import to_dataframe
from taegis_magic.pandas.utils import chunk_list
from taegis_sdk_python.services.alerts.types import GetByIDRequestInput
from taegis_sdk_python.services.queries.types import QLQueriesInput

log = logging.getLogger(__name__)


def inflate_evidence(df: pd.DataFrame) -> pd.DataFrame:
    """Inflate evidence DataFrame by expanding the 'evidence' columns."""
    evidence_columns = {"alerts_evidence", "events_evidence", "search_queries_evidence"}
    if not set(df.columns).issuperset(evidence_columns):
        raise ValueError(
            f"DataFrame must contain the following columns: {evidence_columns}"
        )

    alerts_evidence = pd.json_normalize(df["alerts_evidence"].explode())
    if not alerts_evidence.empty:
        alerts_evidence["taegis_magic.evidence_type"] = InvestigationEvidenceType.Alert
        alerts_evidence["taegis_magic.evidence_id"] = alerts_evidence["alert_id"]

    events_evidence = pd.json_normalize(df["events_evidence"].explode())
    if not events_evidence.empty:
        events_evidence["taegis_magic.evidence_type"] = InvestigationEvidenceType.Event
        events_evidence["taegis_magic.evidence_id"] = events_evidence["event_id"]

    search_query_evidence = pd.json_normalize(df["search_queries_evidence"].explode())
    if not search_query_evidence.empty:
        search_query_evidence["taegis_magic.evidence_type"] = (
            InvestigationEvidenceType.Query
        )
        search_query_evidence["taegis_magic.evidence_id"] = search_query_evidence[
            "search_query"
        ]

    # Reset index to maintain DataFrame integrity
    evidence_df = pd.concat(
        [alerts_evidence, events_evidence, search_query_evidence], axis=0
    )
    evidence_df = evidence_df.dropna(axis=0, how="all").reset_index(drop=True)

    return evidence_df


def lookup_evidence(df: pd.DataFrame, region: str) -> pd.DataFrame:
    """Lookup evidence DataFrame by expanding the 'evidence' columns."""
    if (
        not "taegis_magic.evidence_type" in df.columns
        and not "taegis_magic.evidence_id" in df.columns
    ):
        raise ValueError(
            "DataFrame must contain the following columns: "
            "'taegis_magic.evidence_type', 'taegis_magic.evidence_id'"
        )

    service = get_service(environment=region)

    alerts_evidence = pd.DataFrame()
    events_evidence = pd.DataFrame()
    search_queries_evidence = pd.DataFrame()

    alerts_df = df[df["taegis_magic.evidence_type"] == InvestigationEvidenceType.Alert]
    events_df = df[df["taegis_magic.evidence_type"] == InvestigationEvidenceType.Event]
    queries_df = df[df["taegis_magic.evidence_type"] == InvestigationEvidenceType.Query]

    if not alerts_df.empty:

        for tenant_id in alerts_df["tenant_id"].unique():

            alerts_list = alerts_df[alerts_df["tenant_id"] == tenant_id][
                "taegis_magic.evidence_id"
            ].tolist()

            for chunk in chunk_list(alerts_list, 500):
                try:
                    with service(
                        tenant_id=tenant_id,
                        output="alerts { list { id tenant { id } status metadata { title severity confidence } entities { entities } } }",
                    ):
                        alerts = (
                            service.alerts.query.alerts_service_retrieve_alerts_by_id(
                                in_=GetByIDRequestInput(i_ds=chunk)
                            )
                        )

                    alerts_evidence = pd.concat(
                        [
                            alerts_evidence,
                            to_dataframe(
                                [asdict(alert) for alert in alerts.alerts.list]
                            ),
                        ]
                    )
                except Exception as e:
                    log.error(f"Error retrieving alerts for tenant {tenant_id}: {e}")
                    log.error(f"Alerts list: {chunk}")

        log.debug(f"Alerts evidence columns: {alerts_evidence.columns.tolist()}")
        log.debug(f"Alerts evidence retrieved: {alerts_evidence.head(5)}")

        df = df.merge(
            alerts_evidence,
            how="left",
            left_on="taegis_magic.evidence_id",
            right_on="id",
            suffixes=("", "_alerts"),
        )

    if not events_df.empty:

        for tenant_id in events_df["tenant_id"].unique():

            events_list = events_df[events_df["tenant_id"] == tenant_id][
                "taegis_magic.evidence_id"
            ].tolist()

            for chunk in chunk_list(events_list, 100):
                try:
                    with service(tenant_id=tenant_id):
                        events = service.events.query.events(ids=events_list)

                    events_evidence = pd.concat(
                        [
                            events_evidence,
                            to_dataframe([asdict(event) for event in events]),
                        ]
                    )
                except Exception as e:
                    log.error(f"Error retrieving events for tenant {tenant_id}: {e}")
                    log.error(f"Events list: {chunk}")

        log.debug(f"Events evidence columns: {events_evidence.columns.tolist()}")
        log.debug(f"Events evidence retrieved: {events_evidence.head(5)}")

        df = df.merge(
            events_evidence,
            how="left",
            left_on="taegis_magic.evidence_id",
            right_on="id",
            suffixes=("", "_events"),
        )

    if not queries_df.empty:

        for tenant_id in queries_df["tenant_id"].unique():

            queries_list = queries_df[queries_df["tenant_id"] == tenant_id][
                "taegis_magic.evidence_id"
            ].tolist()

            for chunk in chunk_list(queries_list, 100):
                try:
                    with service(tenant_id=tenant_id):
                        queries = service.queries.query.ql_queries(
                            input_=QLQueriesInput(rns=chunk)
                        )

                    search_queries_evidence = pd.concat(
                        [
                            search_queries_evidence,
                            to_dataframe([asdict(query) for query in queries.queries]),
                        ]
                    )
                except Exception as e:
                    log.error(f"Error retrieving queries for tenant {tenant_id}: {e}")
                    log.error(f"Queries list: {chunk}")

        log.debug(f"Query evidence columns: {search_queries_evidence.columns.tolist()}")
        log.debug(f"Query evidence retrieved: {search_queries_evidence.head(5)}")

        df = df.merge(
            search_queries_evidence,
            how="left",
            left_on="taegis_magic.evidence_id",
            right_on="rn",
            suffixes=("", "_queries"),
        )

    return df
