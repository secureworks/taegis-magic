"""Pandas functions for Search DataFrames."""

import logging

import pandas as pd

log = logging.getLogger(__name__)


def inflate_prompt_examples(df: pd.DataFrame) -> pd.DataFrame:
    """Takes a DataFrame containing Taegis Search Prompts (V2) data and,
    if there is a prompts column with nested data, explodes that data
    into individual rows and normalizes the nested data into prefixed columns.

    Parameters
    ----------
    df : pd.DataFrame
        Search Prompts (V2) DataFrame

    Returns
    -------
    pd.DataFrame
        Returns an Search Prompts (V2) Dataframe with exploded example data

    Raises
    ------
    ValueError
        Dataframe does not contain an prompts column
    """

    if df.empty:
        return df

    if "prompts" not in df.columns:
        raise ValueError("Dataframe does not contain an prompts column")

    if not any(df.columns.str.startswith("example.")):
        df = df.explode("prompts").reset_index(drop=True)

        example_df = pd.json_normalize(df["prompts"]).dropna(axis=1, how="all")

        no_prefix = [col for col in example_df.columns if "example." not in col]

        if no_prefix:
            for column in no_prefix:
                example_df = example_df.rename({column: f"example.{column}"}, axis=1)

        return pd.concat(
            [
                df,
                example_df,
            ],
            axis=1,
        )

    return df
