"""Utility functions for use with Pandas."""

import logging
from typing import List

import pandas as pd

logger = logging.getLogger(__name__)

MAGIC_COLUMN = "taegis_magic.{}"


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
