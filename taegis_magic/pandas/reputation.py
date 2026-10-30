"""Pandas functions for Reputation lookups in Event and Alert DataFrames."""

import logging
from typing import Dict, List, Optional

import pandas as pd
from taegis_sdk_python.services.threat_context.types import (
    ThreatContextIndicatorInput,
)

from taegis_magic.core.service import get_service
from taegis_magic.pandas.utils import chunk_list

log = logging.getLogger(__name__)

REPUTATION_CHUNK_SIZE = 100
REPUTATION_COLUMN_PREFIX = "reputation"

# Event fields for the @ip, @domain and @hash logical types.
# https://docs.taegis.secureworks.com/search/data_lake_search/logical_types/
# The detection ("entities" prefix) fields are not included.
IP_FIELDS = [
    "source_address",
    "destination_address",
    "target_address",
    "true_source_address",
    "source_nat_address",
    "destination_nat_address",
]
DOMAIN_FIELDS = [
    "query_name",
    "target_domain_name",
    "source_domain_name",
    "extra_targetoutbounddomainname",
]
HASH_FIELDS = [
    "process_file_hash",
    "process_file_hash.md5",
    "process_file_hash.sha1",
    "process_file_hash.sha256",
    "process_file_hash.sha512",
    "parent_process_file_hash.md5",
    "parent_process_file_hash.sha1",
    "parent_process_file_hash.sha256",
    "parent_process_file_hash.sha512",
    "file_hash",
    "file_hash.md5",
    "file_hash.sha1",
    "file_hash.sha256",
    "file_hash.sha512",
    "program_hash.md5",
    "program_hash.sha1",
    "program_hash.sha256",
    "program_hash.sha512",
    "target_program.sha1_hash",
    "host_program.sha1_hash",
]


def _existing_columns(df: pd.DataFrame, fields: List[str]) -> List[str]:
    """Resolve fields to dataframe columns.

    A field resolves to itself if present, otherwise to `event_data.<field>`
    if that is present.  Fields with neither column are skipped.
    """
    columns = []
    for field in fields:
        for candidate in (field, f"event_data.{field}"):
            if candidate in df.columns:
                columns.append(candidate)
                break
    return columns


def _as_indicator_series(series: pd.Series) -> pd.Series:
    """Strip strings and null out everything that isn't a non-empty string."""
    return series.map(
        lambda x: x.strip() if isinstance(x, str) and x.strip() else None
    )


def _lookup_indicators(
    indicators: List[str], region: Optional[str]
) -> Dict[str, dict]:
    """Run indicators against the reputation query in chunks.

    Returns a mapping of indicator to its flattened reputation result.
    """
    service = get_service(environment=region)
    results: Dict[str, dict] = {}

    for chunk in chunk_list(indicators, REPUTATION_CHUNK_SIZE):
        try:
            response = service.threat_context.query.reputation(
                indicators=[ThreatContextIndicatorInput(indicator=x) for x in chunk]
            )
        except Exception as exc:  # pylint: disable=broad-except
            log.warning(
                "Reputation lookup failed for %s indicators: %s", len(chunk), exc
            )
            continue

        for result in response:
            if not result.indicator:
                continue
            record = result.to_dict()
            record.pop("id", None)
            record.pop("indicator", None)
            results[result.indicator] = record

    return results


def lookup_reputation(
    df: pd.DataFrame,
    ip_columns: Optional[List[str]] = None,
    domain_columns: Optional[List[str]] = None,
    hash_columns: Optional[List[str]] = None,
    additional_ip_columns: Optional[List[str]] = None,
    additional_domain_columns: Optional[List[str]] = None,
    additional_hash_columns: Optional[List[str]] = None,
    region: Optional[str] = None,
) -> pd.DataFrame:
    """Add Taegis reputation data for IP, domain and hash columns to a dataframe.

    All values from the selected columns are deduplicated, looked up in chunks
    of 100 against the threat_context reputation query and joined back onto the
    dataframe by indicator.  For every source column `<col>`, results are added
    as `reputation.<col>.<field>` columns.

    Intended for use with `DataFrame.pipe`.

    Parameters
    ----------
    df : pd.DataFrame
        Dataframe containing indicators.
    ip_columns : Optional[List[str]]
        IP columns.  Defaults to the @ip logical type fields present in the
        dataframe, as `<field>` or `event_data.<field>`.
    domain_columns : Optional[List[str]]
        Domain columns.  Defaults to the @domain logical type fields present.
    hash_columns : Optional[List[str]]
        Hash columns.  Defaults to the @hash logical type fields present.
    additional_ip_columns : Optional[List[str]]
        Extra IP columns added to ip_columns.
    additional_domain_columns : Optional[List[str]]
        Extra domain columns added to domain_columns.
    additional_hash_columns : Optional[List[str]]
        Extra hash columns added to hash_columns.
    region : Optional[str]
        Taegis SDK region.

    Returns
    -------
    pd.DataFrame
        Copy of the dataframe with reputation columns added.

    Raises
    ------
    ValueError
        If an explicitly requested column is not in the dataframe.
    """
    df = df.copy()

    if ip_columns is None:
        ip_columns = _existing_columns(df, IP_FIELDS)
    if domain_columns is None:
        domain_columns = _existing_columns(df, DOMAIN_FIELDS)
    if hash_columns is None:
        hash_columns = _existing_columns(df, HASH_FIELDS)

    columns: List[str] = []
    for column in [
        *ip_columns,
        *(additional_ip_columns or []),
        *domain_columns,
        *(additional_domain_columns or []),
        *hash_columns,
        *(additional_hash_columns or []),
    ]:
        if column not in columns:
            columns.append(column)

    missing = [x for x in columns if x not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in dataframe: {missing}")

    if df.empty or not columns:
        return df

    keys = {column: _as_indicator_series(df[column]) for column in columns}

    indicators = sorted(
        {value for series in keys.values() for value in series.dropna().unique()}
    )
    if not indicators:
        return df

    reputation = _lookup_indicators(indicators, region)
    if not reputation:
        log.warning("No reputation results returned.")
        return df

    reputation_df = pd.json_normalize(
        [{"indicator": k, **v} for k, v in reputation.items()]
    ).set_index("indicator")

    joined = []
    for column in columns:
        part = (
            keys[column]
            .to_frame(column)
            .join(reputation_df, on=column)
            .drop(columns=column)
            .add_prefix(f"{REPUTATION_COLUMN_PREFIX}.{column}.")
        )
        joined.append(part)

    return pd.concat([df, *joined], axis=1).dropna(how="all", axis=1)
