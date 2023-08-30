"""Taegis Tenants pandas functions."""

from typing import Any, Dict, List

import pandas as pd


def inflate_environments(df: pd.DataFrame) -> pd.DataFrame:
    """Inflate environments to columns.

    Parameters
    ----------
    df : pd.DataFrame
        Tenants DateFrame

    Returns
    -------
    pd.DataFrame
        Inflated Tenants DateFrame

    Raises
    ------
    ValueError
        'environments' column not found in DataFrame
    """
    if df.empty:
        return df

    if "environments" not in df.columns:
        raise ValueError("'environments' column not found in DataFrame")

    def environments_to_dict(environments: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Flatten environments array to dictionary key value pairs."""
        env_dict = {}
        for environment in environments:
            env_dict[environment.get("name", "error")] = environment.get(
                "enabled", False
            )
            env_dict[
                f"{environment.get('name', 'error')}.created_at"
            ] = environment.get("created_at", False)
            env_dict[
                f"{environment.get('name', 'error')}.updated_at"
            ] = environment.get("updated_at", False)
        return env_dict

    df = pd.concat(
        [
            df,
            df["environments"]
            .apply(environments_to_dict)
            .apply(pd.Series)
            .fillna(False)
            .add_prefix("environments."),
        ],
        axis=1,
    )

    return df
