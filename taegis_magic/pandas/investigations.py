"""Pandas functions for investigations."""

import pandas as pd
from taegis_magic.commands.utils.investigations import InvestigationEvidenceType


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
