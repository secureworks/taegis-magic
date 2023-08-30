"""Pandas functions for Rules DataFrames."""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def inflate_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Takes a DataFrame containing Taegis Event Filter
    Rules and concats an exploded JSON blob containing
    the Event Filter criteria.

    Parameters
    ----------
    df : pd.DataFrame
        Rules DataFrame

    Returns
    -------
    pd.DataFrame
        Inflated Rules DataFrame

    Raises
    ------
    ValueError
        DataFrame must have a 'filters' column.

    Example
    -------
    Example::

        %taegis rules suppression --event-type cloudaudit --assign df

        inflated_df = df.pipe(inflate_filters)
    """
    if "filters.id" in df.columns:
        return df

    if "filters" not in df.columns:
        raise ValueError("DataFrame must have a 'filters' column.")

    df = df.explode("filters").reset_index(drop=True)
    return pd.concat(
        [
            df,
            pd.json_normalize(
                df["filters"].apply(lambda x: {} if pd.isnull(x) else x).to_list(),
                max_level=3,
            ).add_prefix("filters."),
        ],
        axis=1,
    )
