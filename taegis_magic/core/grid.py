"""Taegis Magic ipydatagrid widget."""

import json
import logging
from enum import Enum
from typing import Any, Dict, Optional

import pandas as pd
from ipydatagrid import DataGrid, HyperlinkRenderer, VegaExpr

log = logging.getLogger(__name__)


def validate_data_map(value: Any) -> Any:
    """Certain data types show as [object Object] in the datagrid.  This function will convert them to a string."""
    if isinstance(value, Enum):
        try:
            return value.value
        except Exception as e:
            log.error(f"Error converting Enum ({value}) to value: {e}")
            return "Error"

    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value)
        except Exception as e:
            log.error(f"Error converting JSON-able ({value}) to value: {e}")
            return "Error"

    return value


def data_grid(
    df: pd.DataFrame,
    *,
    auto_fit_columns: bool = True,
    auto_fit_params: Optional[Dict[str, Any]] = None,
    validate_data: bool = True,
    editable: bool = True,
    limit: Optional[int] = 1000,
    **kwargs,
) -> DataGrid:
    """Default configurations for an ipydatagrid widget.

    Parameters
    ----------
    df : DataFrame
        Pandas DataFrame
    auto_fit_params : Optional[Dict[str, Any]], optional
        Data fit options, by default None
        https://github.com/jupyter-widgets/ipydatagrid/blob/main/examples/Column%20Width%20Auto-Fit.ipynb
    validate_data : bool, optional
        Change data that would present [object Object] to a string value, by default True
    editable : bool, optional
        Allow editing of the data in the widget, by default True
    limit: Optional[int], optional
        Limit the number of rows displayed in the widget, by default 1000
    kwargs: Dict[str, Any], optional
        Keyword arguments for the DataGrid widget

    Returns
    -------
    DataGrid
        ipydatagrid widget
    """
    df = df.copy()
    if limit:
        df = df.head(limit)

    if validate_data:
        # targeting pandas 2.0+, should update to just 'map' when we want to move to 2.1+
        log.debug("Validating dataframe...")
        df = df.applymap(validate_data_map, na_action="ignore")

    if "share_link" in df.columns:
        if "renderers" not in kwargs:
            log.debug("Renderers not found.  Adding default rendererer...")
            kwargs["renderers"] = {}
        if "share_link" not in kwargs["renderers"]:
            log.debug(
                "share_link renderer not found.  Adding default share_link rendererer..."
            )
            share_link_renderer = HyperlinkRenderer(
                url=VegaExpr("cell.value"),
                url_name=VegaExpr("cell.value"),
                text_color="blue",
            )
            kwargs["renderers"].update({"share_link": share_link_renderer})

    log.debug("Building widget...")
    grid = DataGrid(df, editable=editable, **kwargs)
    if auto_fit_columns:
        if not auto_fit_params:
            auto_fit_params = {"area": "body", "padding": 200, "numCols": None}

        grid.auto_fit_params = auto_fit_params
        grid.auto_fit_columns = auto_fit_columns

    log.debug("Returning widget...")
    return grid
