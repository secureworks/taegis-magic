
import logging

import pandas as pd

log = logging.getLogger(__name__)

def inflate_supported_primary_statues(df: pd.DataFrame) -> pd.DataFrame:
    """Expands `supported_primary_statuses` column of a DataFrame and returns
    a new DataFrame where the content of `supported_primary_statuses` is
    represented as columns.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing an `supported_primary_statuses` column
    Returns
    -------
    pd.DataFrame
        DataFrame with `supported_primary_statuses` columns appended

    Example
    -------
    %taegis cases types --assign case_types 

    cases_inflated = case_types.pipe(inflate_supported_primary_statues)
    """
    df = df.copy()

    if df.empty:
        log.warning("DataFrame is empty.")
        return df

    if "supported_primary_statuses" in df.columns:
        if df["supported_primary_statuses"].any():
            df = df.explode("supported_primary_statuses")

            return pd.concat(
                [
                    df,
                    df["supported_primary_statuses"]
                    .apply(pd.Series)
                    .add_prefix("supported_primary_statuses."),
                ],
                axis=1,
            )

        log.warning("supported_primary_statuses column contains no data to be inflated.")
        return df

    log.warning("Dataframe did not contain a supported_primary_statuses column.")
    return df


def inflate_supported_primary_verdicts(df: pd.DataFrame) -> pd.DataFrame:
    """Expands `supported_primary_verdicts` column of a DataFrame and returns
    a new DataFrame where the content of `supported_primary_verdicts` is
    represented as columns.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing an `supported_primary_verdicts` column
    Returns
    -------
    pd.DataFrame
        DataFrame with `supported_primary_verdicts` columns appended

    Example
    -------
    %taegis cases types --assign case_types 

    cases_inflated = case_types.pipe(inflate_supported_primary_verdicts)
    """
    df = df.copy()

    if df.empty:
        log.warning("DataFrame is empty.")
        return df

    if "supported_primary_verdicts" in df.columns:
        if df["supported_primary_verdicts"].any():
            df = df.explode("supported_primary_verdicts")

            return pd.concat(
                [
                    df,
                    df["supported_primary_verdicts"]
                    .apply(pd.Series)
                    .add_prefix("supported_primary_verdicts."),
                ],
                axis=1,
            )

        log.warning("supported_primary_verdicts column contains no data to be inflated.")
        return df

    log.warning("Dataframe did not contain a supported_primary_verdicts column.")
    return df
