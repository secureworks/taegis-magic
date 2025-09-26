"""Utility functions for use with Pandas."""

import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)

MAGIC_COLUMN = "taegis_magic.{}"


DEFAULT_COLUMNS = {
    "agentdetection": [
        "host_id",
        "hostname",
        "detection_category",
        "detection_type",
        "image_path",
        "summary",
    ],
    "antivirus": [
        "host_id",
        "action_taken",
        "threat_category",
        "threat_name",
        "user_name",
        "file_path",
    ],
    "apicall": [
        "host_id",
        "hostname",
        "function_called",
        "was_modification_allowed",
        "was_operation_successful",
        "action",
        "commandline",
    ],
    "auth": [
        "source_address",
        "target_user_name",
        "action",
        "auth_system",
        "user_agent",
        "application_name",
    ],
    "cloudaudit": [
        "source_address",
        "user_name",
        "event_type",
        "event_name",
        "mfa_used",
        "user_agent",
    ],
    "detectionfinding": [],
    "dhcp": [
        "host_id",
        "hostname",
        "server_address",
        "client_address",
        "action",
    ],
    "dnsquery": [
        "hostname",
        "os.os",
        "os.arch",
        "processcorrelationid.pid",
        "query_name",
        "query_type",
    ],
    "email": [
        "host_id",
        "direction",
        "status",
        "event_type",
        "from_email_address",
        "subject",
    ],
    "encrypt": [
        "host_id",
        "hostname",
        "source_address",
        "destination_address",
        "tls_version",
    ],
    "filemod": [
        "host_id",
        "hostname",
        "parent_path",
        "process_image_path",
        "action",
        "file_name",
        "process_username",
    ],
    "generic": [
        "host_id",
        "hostname",
        "summary",
    ],
    "http": [
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
    ],
    "managementevent": [
        "host_id",
        "hostname",
        "type",
        "channel",
        "operation",
        "username",
    ],
    "netflow": [
        "hostname",
        "sensor_type",
        "protocol",
        "source_address",
        "destination_address",
        "destination_port",
        "dns_name",
    ],
    "nids": [
        "source_address",
        "destination_address",
        "action",
        "blocked",
        "enrichSummary",
    ],
    "persistence": [
        "host_id",
        "hostname",
        "category",
        "command.program.path",
        "command.args",
    ],
    "process": [
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
    ],
    "processmodule": [
        "host_id",
        "hostname",
        "sensor_action",
        "file.path",
        "module_action",
    ],
    "registry": [
        "host_id",
        "hostname",
        "event_type",
        "path",
        "process_image_path",
        "process_username",
        "value.name",
        "value.data32",
        "value.data64",
    ],
    "scriptblock": [
        "os.os",
        "os.arch",
        "interpreter_name",
        "interpreter_path",
        "script_name",
        "decoded_block_text",
    ],
    "thirdparty": [
        "source_address",
        "user_principal_name",
        "title",
        "ontology",
    ],
    "threadinjection": [
        "host_id",
        "hostname",
        "source_process_name",
        "target_process_name",
        "thread_id",
    ],
}


def get_tenant_id(tenant_id):
    """Coerce tenant ids into common format."""
    if isinstance(tenant_id, int):
        return str(tenant_id)
    elif isinstance(tenant_id, list):
        return str(tenant_id[0])
    elif isinstance(tenant_id, str):
        replacement_chars = ["[", "]", "'", '"']
        for replacement_char in replacement_chars:
            tenant_id = tenant_id.replace(replacement_char, "")
        return tenant_id
    else:
        raise ValueError(f"{tenant_id} is invalid format")


def chunk_list(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def coalesce_columns(df: pd.DataFrame, columns: List[str]) -> pd.Series:
    """Reduce results to the first result in list of columns."""
    from functools import reduce

    return reduce(
        lambda left, right: left.combine_first(right),
        [df[col] for col in columns if col in df.columns],
    )


def return_valid_column(df: pd.DataFrame, column_list: List[str]) -> pd.Series:
    """Takes a dataframe and a list of user defined columns to obtain the first possible valid column.

    Parameters
    ----------
    df : pd.DataFrame
        A pandas dataframe.
    column_list : List[str]
        A list of columns a user defines to pull out the first possible valid column
        from the dataframe that is passed.

    Returns
    -------
    pd.Series
        Returns a pandas Series of the first column it was able to obtain from the dataframe passed in.

    Raises
    ------
    ValueError
        Returns if there are no valid columns found within the dataframe.
    """
    valid_columns = []

    if not column_list or not isinstance(column_list, list):
        raise ValueError("Provided column list is either blank or not a list.")

    for col in column_list:
        if col in df.columns:
            valid_columns.append(col)
            logger.debug(f"Found identifier column: {col}")

    if not valid_columns:
        raise ValueError(
            f"DataFrame does not contain any columns from supplied list: {column_list}"
        )

    return coalesce_columns(df, valid_columns)


def drop_duplicates_by_hashables(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop duplicates from a Pandas DataFrame based upon it's hashable columns.

    Parameters
    ----------
    df : pd.DataFrame
        Pandas DataFrame

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame
    """
    hashable_columns = []
    for column in df.columns:
        try:
            df.drop_duplicates([df.columns[0], column])
        except TypeError:
            # log.debug(f"{column} is non-hashable skipping...")
            continue

        hashable_columns.append(column)

    return df.copy().drop_duplicates(hashable_columns)


def groupby(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Validate column list against DataFrame and return a standardized Pandas DataFrame goupby operation.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame to group by.
    columns : List[str]
        List of columns to group by.

    Returns
    -------
    pd.DataFrame
        DataFrame with grouped counts.
    """
    for column in columns.copy():
        if not column in df.columns:
            logger.error(f"Column {column} not found in dataframe")
            columns.remove(column)
    return (
        df[columns]
        .astype(str)
        .groupby(columns, dropna=False)
        .size()
        .reset_index(name="count")
    )


def default_schema_columns(schema: str) -> List[str]:
    """Return default Magic columns for a given schema.

    Parameters
    ----------
    schema : str
        Schema name.

    Returns
    -------
    List[str]
        List of default columns.
    """
    return DEFAULT_COLUMNS.get(schema.lower(), [])
