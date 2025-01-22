"""Pandas functions for Alerts DataFrames."""

import logging
import contextlib
import json
from typing import Dict, List, Optional

import pandas as pd
from taegis_magic.pandas.utils import coalesce_columns

log = logging.getLogger(__name__)


def convert_event_timestamps(
    df: pd.DataFrame, format_: str = "%Y-%m-%dT%H:%M:%SZ"
) -> pd.DataFrame:
    """Takes an Events dataframe and converts all the metadata time columns
    into a readable time format. All columns with usec or us will be dropped to reduce
    the amount of columns in the dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        Events Dataframe
    format : str, optional
        Datetime string format to be used, by default "%Y-%m-%dT%H:%M:%SZ"

    Returns
    -------
    pd.DataFrame
        Returns Events Dataframe with added taegis_magic timestamp columns
    """

    if df.empty:
        return df

    df = df.copy()

    for column in [
        column
        for column in df.columns
        if column.endswith("_time_usec")
        or column.endswith("mod_time_us")
        and not column.startswith("taegis_magic.")
    ]:
        try:
            df[f"taegis_magic.{column}"] = pd.to_datetime(
                df[column], errors="ignore", unit="us"
            ).dt.strftime(format_)
        except Exception as exc:
            log.error(exc)
            continue

        df[f"taegis_magic.{column}"] = df[f"taegis_magic.{column}"].fillna("N/A")

    return df


def inflate_original_data(
    df: pd.DataFrame, original_data_columns: Optional[List[str]] = None
) -> pd.DataFrame:
    """Takes a DataFrame containing a valid Event original_data column
    and concats the nested JSON blob containing the
    event original data as columns. Returns a DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Taegis Alerts or Events Query Dataframe
    original_data_columns : Optional[List[str]], optional
        A list of columns that contain Taegis Event original data, by default None

    Returns
    -------
    pd.DataFrame
        Dataframe that contains inflated Event original_data columns
    """

    def load_json(event):
        if isinstance(event, str):
            with contextlib.suppress(Exception):
                return json.loads(event)
        return {}

    if df.empty:
        return df

    if original_data_columns is None:
        original_data_columns = [
            "original_data",
            "event_data.original_data",
        ]

    valid_original_data_columns = []

    for original_data_col in original_data_columns:
        if original_data_col in df.columns:
            valid_original_data_columns.append(original_data_col)

    if not valid_original_data_columns:
        raise ValueError(
            f"DataFrame does not contain a vaild original data column: {original_data_columns}"
        )

    if any(df.columns.str.startswith("original_data.")) is False:

        return pd.concat(
            [
                df,
                pd.json_normalize(
                    coalesce_columns(df, valid_original_data_columns).apply(load_json)
                ).add_prefix("original_data."),
            ],
            axis=1,
        )

    return df


def inflate_schema_keys(df: pd.DataFrame) -> pd.DataFrame:
    """Parse and format event schema keys.

    Expected Format: 'scwx.schema.key'

    Parameters
    ----------
    df : pd.DataFrame
        Schema Keys

    Returns
    -------
    pd.DataFrame
        Schema Keys with formatted keys
    """
    if df.empty:
        return df

    if "key" not in df.columns:
        raise ValueError("DataFrame does not contain a 'key' column")

    df = df.copy()

    def split_key(key: str, index: int) -> str:
        if not isinstance(key, str):
            return "Error"

        parts = key.split(".", maxsplit=2)

        try:
            part = parts[index]
        except IndexError:
            return "Error"

        return part

    df["taegis_magic.schema"] = df["key"].apply(split_key, index=1)
    df["taegis_magic.key"] = df["key"].apply(split_key, index=2)

    return df
