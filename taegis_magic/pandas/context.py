"""Context gathering functions."""

from typing import Callable, Dict, List, Optional

import pandas as pd
import numpy as np
import panel as pn
from IPython.core.display import display
from taegis_magic.pandas.alerts import inflate_raw_events
from taegis_sdk_python import GraphQLService

pn.extension("tabulator")


def normalize_entities(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize entity values from alerts for Taegis Query Language logical types:

        '@ip', '@domain', '@hash', '@host', '@user'.

    https://docs.ctpx.secureworks.com/search/querylanguage/advanced_search/#logical-types

    Parameters
    ----------
    df : pd.DataFrame
        Alert DataFrame

    Returns
    -------
    pd.DataFrame
        Normalized Entity DataFrame
    """
    df = df.copy()

    df = df.explode("entities.entities")

    def normalize_entity(entity):
        entity_type, value = entity.split(":", 1)

        logical_types_map = {
            "@ip": [
                "ipaddress",
                "sourceIpAddress",
                "ipAddress",
                "destIpAddress",
                "sourceIpGeo",
                "destIpGeo",
                "receiverIp",
                "senderIp",
                "sourceAddress",
                "targetIp",
                "targetIpAddress",
                "destAddress",
                "destinationAddress",
            ],
            "@domain": [
                "dnsName",
                "domainname",
                "ipDomain",
                "topPrivateIpDomain",
                "domainName",
                "queryName",
                "sourceHostnameFqdn",
                "targetHostnameFqdn",
                "uriHost",
                "domain",
            ],
            "@hash": ["md5", "sha1", "sha256", "sha512"],
            "@host": [
                "sourceHostName",
                "destHostName",
                "workstationName",
                "targetHostName",
                "hostName",
                "computerName",
            ],
            "@user": ["userName", "username", "sourceUserName", "targetUserName"],
        }

        for logical_type_key, logical_type_values in logical_types_map.items():
            if entity_type in logical_type_values:
                return pd.Series(
                    [logical_type_key, value],
                    index=[
                        "taegis_magic.entities.field",
                        "taegis_magic.entities.value",
                    ],
                )

        return pd.Series(
            [None, None],
            index=["taegis_magic.entities.field", "taegis_magic.entities.value"],
        )

    df2 = (
        pd.concat([df, df["entities.entities"].apply(normalize_entity)], axis=1)
        .reset_index(drop=True)
        .dropna(
            subset=["taegis_magic.entities.field", "taegis_magic.entities.value"],
            how="all",
        )
        .drop_duplicates(
            subset=["id", "taegis_magic.entities.field", "taegis_magic.entities.value"]
        )
    )

    return df2


def relate_entities(df: pd.DataFrame) -> pd.DataFrame:
    """Relate logical entities to lists of indicators.

    Parameters
    ----------
    df : pd.DataFrame
        Normalized Entity DataFrame

    Returns
    -------
    pd.DataFrame
        Releted Entity DataFrame

    Raises
    ------
    ValueError
        column not found, run pipe function 'normalize_entities'
    """
    if df.empty:
        return df

    if "taegis_magic.entities.field" not in df.columns:
        raise ValueError(
            "taegis_magic.entities.field column not found, run pipe function 'normalize_entities'"
        )

    if "taegis_magic.entities.value" not in df.columns:
        raise ValueError(
            "taegis_magic.entities.value column not found, run pipe function 'normalize_entities'"
        )

    df = df.copy()

    groupby_alerts = (
        df.groupby(["taegis_magic.entities.field", "taegis_magic.entities.value"])["id"]
        .unique()
        .reset_index()
    )
    groupby_size = (
        df.groupby(["id", "taegis_magic.entities.field", "taegis_magic.entities.value"])
        .size()
        .reset_index()
    )

    def entity_relationships(entity: str, field: str):
        return (
            groupby_size[
                (
                    groupby_size["id"].isin(
                        groupby_alerts[
                            groupby_alerts["taegis_magic.entities.value"] == entity
                        ]["id"]
                        .explode()
                        .unique()
                    )
                )
                & (groupby_size["taegis_magic.entities.value"] != entity)
                & (groupby_size["taegis_magic.entities.field"] == field)
            ]["taegis_magic.entities.value"]
            .unique()
            .tolist()
        )

    for field in df["taegis_magic.entities.field"].unique():
        df[field] = df["taegis_magic.entities.value"].apply(
            entity_relationships, field=field
        )

    return df.drop_duplicates("taegis_magic.entities.value")


def generate_context_queries(
    df: pd.DataFrame,
    open_alerts_timeframe="EARLIEST=-1d",
    resolved_alerts_timeframe="EARLIEST=-30d",
    investigations_timeframe="EARLIEST=-30d",
    events_timeframe="EARLIEST=-1d",
) -> pd.DataFrame:
    """Generate Taegis Query Language queries based on logical entities.

    Parameters
    ----------
    df : pd.DataFrame
        Normalized Entities DataFrame

    Returns
    -------
    pd.DataFrame
        Generate Query DataFrame
    """

    if df.empty:
        return df

    if "taegis_magic.entities.field" not in df.columns:
        raise ValueError(
            "taegis_magic.entities.field column not found, run pipe function 'normalize_entities'"
        )

    if "taegis_magic.entities.value" not in df.columns:
        raise ValueError(
            "taegis_magic.entities.value column not found, run pipe function 'normalize_entities'"
        )

    df = df.copy()

    def generate_context_from_row(row):
        query_params = [
            f"{row['taegis_magic.entities.field']} = '{row['taegis_magic.entities.value']}'"
        ]

        title = row["metadata.title"]

        for idx in row.index:
            if "@" in idx and row[idx]:
                for item in row[idx]:
                    query_params.append(f"{idx} = '{item}'")

        open_alerts_query = f"""
        FROM alert
        WHERE
            ({' OR '.join(query_params)}) AND
            metadata.title != '{title}' AND
            status = 'OPEN' AND
            investigation_ids IS NULL
        {open_alerts_timeframe}
        """

        resolved_alerts_query = f"""
        FROM alert
        WHERE
            ({' OR '.join(query_params)}) AND
            status != 'OPEN'
        {resolved_alerts_timeframe} | aggregate count by metadata.title, entities, status, resolution_reason
        """

        investigations_query = f"""
        FROM alert
        WHERE
            ({' OR '.join(query_params)}) AND
            investigation_ids IS NOT NULL
        {investigations_timeframe} | aggregate count by metadata.title, entities, investigation_ids, status
        """

        events_query = f"""
        WHERE
            ({' OR '.join(query_params)})
        {events_timeframe}
        """

        return pd.Series(
            [
                open_alerts_query.strip(),
                resolved_alerts_query.strip(),
                investigations_query.strip(),
                events_query.strip(),
            ],
            index=[
                "taegis_magic.open_alerts_query",
                "taegis_magic.resolved_alerts_query",
                "taegis_magic.investigations_query",
                "taegis_magic.events_query",
            ],
        )

    df2 = pd.concat(
        [df, df.apply(generate_context_from_row, axis=1)], axis=1
    ).reset_index(drop=True)

    return df2


def get_facet(df: pd.DataFrame, columns: List[str], title: str) -> pn.Card:
    for _ in columns.copy():
        try:
            df = (
                df.groupby(columns)
                .size()
                .rename("count")
                .sort_values(ascending=False)
                .to_frame()
                .reset_index()
            )
        except (KeyError, TypeError) as exc:
            columns.remove(exc.args[0])
            continue

        break

    header_filters = {
        column: {"type": "input", "func": "like", "placeholder": "Filter"}
        for column in df.columns
    }

    widget = pn.widgets.Tabulator(
        df,
        width=1050,
        height=500,
        formatters={"bool": {"type": "tickCross"}},
        layout="fit_data",
        theme="bootstrap5",
        theme_classes=["thead-dark", "table-sm"],
        # pagination="local",
        # page_size=50,
        header_filters=header_filters,
    )
    card = pn.Card(widget, title=title)
    return card


def display_facets(
    queries: Dict[str, pd.DataFrame], additional_columns: Optional[List[str]] = None
):
    """Display Card facets for each entity.

    Parameters
    ----------
    queries : Dict[str, pd.DataFrame]
        The DataFrame results from the entity queries.
    additional_columns : Optional[List[str]], optional
        Additional columns to display on each facet (usually provided by add_threat_intel), by default None
    """
    for entity in queries:
        if not queries[entity]["open_alerts"].empty:
            open_alerts = get_facet(
                df=queries[entity]["open_alerts"].explode("entities.entities"),
                columns=[
                    "metadata.title",
                    "entities.entities",
                ],
                title="Open Alerts",
            )
        else:
            open_alerts = pn.Card(title="Open Alerts")

        if not queries[entity]["investigations"].empty:
            investigations = get_facet(
                queries[entity]["investigations"],
                columns=[
                    "metadata.title",
                    "entities",
                    "investigation_ids",
                    "status",
                ],
                title="Investigations",
            )
        else:
            investigations = pn.Card(title="Investigations")

        if not queries[entity]["resolved_alerts"].empty:
            resolved_alerts = get_facet(
                df=queries[entity]["resolved_alerts"],
                columns=["metadata.title", "entities", "status", "resolution_reason"],
                title="Resolved Alerts",
            )
        else:
            resolved_alerts = pn.Card(title="Resolved Alerts")

        schema_cards = []

        try:
            schemas = (
                queries[entity]["events"]["resource_id"]
                .apply(lambda x: x.split(":")[2].split(".")[1])
                .unique()
            )
        except Exception:
            schemas = []

        for schema in schemas:
            df = queries[entity]["events"][
                queries[entity]["events"]["resource_id"].str.contains(schema)
            ].dropna(how="all", axis=1, inplace=False)
            if schema == "auth":
                columns = [
                    "source_address",
                    "target_user_name",
                    "action",
                    "auth_system",
                    "user_agent",
                    "application_name",
                ]
            elif schema == "thirdpartyalert":
                columns = [
                    "source_address",
                    "user_principal_name",
                    "title",
                    "ontology",
                ]
            elif schema == "dnsquery":
                columns = [
                    "hostname",
                    "os.os",
                    "os.arch",
                    "processcorrelationid.pid",
                    "query_name",
                    "query_type",
                ]
            elif schema == "cloudaudit":
                columns = [
                    "source_address",
                    "user_name",
                    "event_type",
                    "event_name",
                    "mfa_used",
                    "user_agent",
                ]
            elif schema == "http":
                columns = [
                    "source_username",
                    "source_address",
                    "destination_address",
                    "destination_port",
                    "user_agent",
                    "http_method",
                    "response_code",
                    "uri_host",
                    "uri_path",
                    "tx_byte_count",
                    "rx_byte_count",
                ]
            elif schema == "netflow":
                columns = [
                    "hostname",
                    "sensor_type",
                    "protocol",
                    "source_address",
                    "destination_address",
                    "destination_port",
                    "dns_name",
                ]
            elif schema == "nids":
                columns = [
                    "source_address",
                    "destination_address",
                    "action",
                    "blocked",
                    "enrichSummary",
                ]
            elif schema == "process":
                columns = [
                    "hostname",
                    "os.os",
                    "os.arch",
                    "sensor_type",
                    "username",
                    "user_is_admin",
                    "process_is_admin",
                    "image_path",
                    "commandline",
                    "was_blocked",
                ]
            elif schema == "scriptblock":
                df["decoded_block_text_truncated"] = df["decoded_block_text"].apply(
                    lambda x: x[:200]
                )
                columns = [
                    "os.os",
                    "os.arch",
                    "interpreter_name",
                    "interpreter_path",
                    "script_name",
                    "decoded_block_text_truncated",
                ]
            else:
                columns = list(df.columns)

            if additional_columns:
                columns.extend(additional_columns)

            schema_cards.append(get_facet(df=df, columns=columns, title=schema))

        display(
            pn.Card(
                open_alerts,
                investigations,
                resolved_alerts,
                *schema_cards,
                title=entity,
            )
        )


def add_threat_intel(
    df: pd.DataFrame,
    correlations: List[Callable],
    tenant_id: Optional[str] = None,
    region: Optional[str] = None,
) -> pd.DataFrame:
    """Correlate Threat Indicators to logical types.

    Parameters
    ----------
    df : pd.DataFrame
        Alerts/Events DataFrame.
    correlations : List[Callable]
        List of correlation functions.
    tenant_id : Optional[str]
        Teanant ID.
    region : Optional[str]
        Taegis Region.

    Returns
    -------
    pd.DataFrame
        Correlated DataFrame.

    Raises
    ------
    ValueError
        resource_id or event_data.resource_id not found in DataFrame.
    """
    if df.empty:
        return df

    df = df.copy()

    column = ""
    prefix = ""
    if "event_ids" in df.columns:
        df = df.pipe(inflate_raw_events)
        prefix = "event_data."

    column = f"{prefix}resource_id"

    if column not in df.columns:
        return df

    # https://docs.ctpx.secureworks.com/search/builder/advanced_search/#logical-type-mappings
    # we only need to define for @ip, @domain, and @hash
    ip_field_map = {
        "auth": [
            "target_address",
            "source_address",
        ],
        "cloudaudit": [
            "source_address",
        ],
        "dnsquery": [
            "source_address",
            "destination_address",
        ],
        "http": [
            "source_address",
            "destination_address",
            "true_source_address",
        ],
        "netflow": [
            "source_address",
            "destination_address",
            "source_nat_address",
            "destination_nat_address",
        ],
        "nids": [
            "source_address",
            "destination_address",
        ],
    }

    domain_field_map = {
        "auth": [
            "target_domain_name",
            "source_domain_name",
            "extra_targetoutbounddomainname",
        ],
        "dnsquery": [
            "query_name",
        ],
    }

    hash_field_map = {
        "auth": [
            "process_file_hash",
            "process_file_hash.md5",
            "process_file_hash.sha1",
            "process_file_hash.sha256",
            "process_file_hash.sha512",
        ],
        "filemod": [
            "file_hash,parent_process_file_hash.md5",
            "parent_process_file_hash.sha1",
            "parent_process_file_hash.sha256",
            "parent_process_file_hash.sha512",
            "process_file_hash.md5",
            "process_file_hash.sha1",
            "process_file_hash.sha256",
            "process_file_hash.sha512",
            "file_hash.md5",
            "file_hash.sha1",
            "file_hash.sha256",
            "file_hash.sha512",
        ],
        "process": [
            "program_hash.md5",
            "program_hash.sha1",
            "program_hash.sha256",
            "program_hash.sha512",
            "target_program.sha1_hash",
            "host_program.sha1_hash",
        ],
    }

    def parse_fields(row, field_map, prefix: str = ""):
        for schema, field_list in field_map.items():
            if schema in row[f"{prefix}resource_id"]:
                l = list(
                    set(
                        [
                            row[f"{prefix}{field}"]
                            for field in field_list
                            if f"{prefix}{field}" in row
                        ]
                    )
                )
                if np.nan in l:
                    l.remove(np.nan)
                return l
        return []

    df["logicals.ip"] = df.apply(
        parse_fields, field_map=ip_field_map, prefix=prefix, axis=1
    )
    df["logicals.domain"] = df.apply(
        parse_fields, field_map=domain_field_map, prefix=prefix, axis=1
    )
    df["logicals.hash"] = df.apply(
        parse_fields, field_map=hash_field_map, prefix=prefix, axis=1
    )

    logical_ips = set()
    logical_domains = set()
    logical_hashes = set()

    for _, row in df.iterrows():
        logical_ips.update(row["logicals.ip"])
        logical_domains.update(row["logicals.domain"])
        logical_hashes.update(row["logicals.hash"])

    indicators = logical_ips | logical_domains | logical_hashes

    for callable in correlations:
        df = df.pipe(
            callable, indicators=indicators, tenant_id=tenant_id, region=region
        )

    return df


def get_ti_pubs(
    df: pd.DataFrame,
    indicators: List[str],
    tenant_id: Optional[str] = None,
    region: Optional[str] = None,
) -> pd.DataFrame:
    """Correlate Threat Indicators to Threat Intelligence Publications.

    Parameters
    ----------
    df : pd.DataFrame
        Alerts/Events DataFrame.
    indicators : List[str]
        List of indicators; IPs, Domains, Hashes.
    tenant_id : Optional[str], optional
        Taegis Tenant ID, by default None
    region : Optional[str], optional
        Taegis Region, by default None

    Returns
    -------
    DataFrame
        Correlated DataFrame.
    """
    df = df.copy()

    service = GraphQLService(tenant_id=tenant_id, environment=region)

    # get TI Pubs
    ti_pubs = {}
    for indicator in indicators:
        with service(output="id Type Name"):
            results = service.threat.query.threat_publications(indicator)
        ti_pubs[indicator] = results
    # ti_pubs

    # correlate
    def check_tips(row, ti_pubs):
        for indicator, pub_list in ti_pubs.items():
            for col in [idx for idx in row.index if idx.startswith("logicals.")]:
                if pub_list and indicator in row[col]:
                    return pd.Series([True, pub_list])
        return pd.Series([False, []])

    df[["tips.found", "tips.publications"]] = df.apply(
        check_tips, ti_pubs=ti_pubs, axis=1
    )

    return df
