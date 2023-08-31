"""Pandas function for Audits DataFrames."""

import difflib
import logging

import pandas as pd
import warnings

warnings.simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

logger = logging.getLogger(__name__)


def get_diffs(df: pd.DataFrame) -> pd.DataFrame:
    """Pull out differences in before_state and after_state.

    Parameters
    ----------
    df : pd.DataFrame
        Audits DataFrame

    Returns
    -------
    pd.DataFrame
        Audits with before_state and after_state diff side by side.

    Example
    -------
    Example::

        %taegis audits search --application investigations --assign search_audits

        search_audits = search_audits.pipe(get_diffs)

    """
    if df.empty:
        return df

    df = df.copy()

    def get_diff(row, col_name: str):
        differ = difflib.Differ()

        if isinstance(row[f"before_state.{col_name}"], list):
            before_state = [str(item) for item in row[f"before_state.{col_name}"]]
        else:
            before_state = str(row[f"before_state.{col_name}"]).splitlines(
                keepends=True
            )

        if isinstance(row[f"after_state.{col_name}"], list):
            after_state = [str(item) for item in row[f"after_state.{col_name}"]]
        else:
            after_state = str(row[f"after_state.{col_name}"]).splitlines(keepends=True)

        diff = list(
            differ.compare(
                before_state,
                after_state,
            )
        )
        return [
            item
            for item in diff
            if item.startswith("-") or item.startswith("+") or item.startswith("?")
        ]

    before_state_columns = {
        column.replace("before_state.", "")
        for column in df.columns
        if column.lower().startswith("before_state")
    }
    after_state_columns = {
        column.replace("after_state.", "")
        for column in df.columns
        if column.lower().startswith("after_state")
    }
    if not before_state_columns:
        raise ValueError("DataFrame does not have a before_state")
    if not after_state_columns:
        raise ValueError("DataFrame does not have an after_state")

    diff_columns = list(before_state_columns.intersection(after_state_columns))

    if not diff_columns:
        raise ValueError(
            "DataFrame does not have any intersecting before_state to after_state columns"
        )

    for col in diff_columns:
        df[f"before_state.{col}"] = df[f"before_state.{col}"].fillna("")

        df[f"after_state.{col}"] = df[f"after_state.{col}"].fillna("")

    for column in diff_columns:
        column_name = f"taegis_magic.diff.{column}"
        if column_name not in df.columns:
            logger.debug("Getting difference on %s...", column_name)
            df[column_name] = df.apply(get_diff, args=(column,), axis=1)
        else:
            logger.debug("%s found, moving to next column...", column_name)

    return df
