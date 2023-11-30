"""Context gathering functions."""

import pandas as pd
from IPython.core.display import display
from typing import Dict, List
import panel as pn

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


def generate_context_queries(df: pd.DataFrame) -> pd.DataFrame:
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
        EARLIEST=-1d
        """

        resolved_alerts_query = f"""
        FROM alert
        WHERE
            ({' OR '.join(query_params)}) AND
            status != 'OPEN'
        EARLIEST=-30d | aggregate count by metadata.title, entities, status, resolution_reason
        """

        investigations_query = f"""
        FROM alert
        WHERE
            ({' OR '.join(query_params)}) AND
            investigation_ids IS NOT NULL
        EARLIEST=-30d | aggregate count by metadata.title, entities, investigation_ids, status
        """

        events_query = f"""
        WHERE
            ({' OR '.join(query_params)})
        EARLIEST=-1d
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

        return pd.DataFrame()

    df2 = pd.concat(
        [df, df.apply(generate_context_from_row, axis=1)], axis=1
    ).reset_index(drop=True)

    return df2


def get_facet(df: pd.DataFrame, columns: List[str], title: str) -> pn.Card:
    df = (
        df.groupby(columns)
        .size()
        .rename("count")
        .sort_values(ascending=False)
        .to_frame()
        .reset_index()
    )

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


def display_facets(queries: Dict[str, pd.DataFrame]):
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
            ].dropna(how="all", axis=1)
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
