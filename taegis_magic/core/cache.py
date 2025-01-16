"""Cache data within IPython output cells."""

import base64
import logging
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import compress_pickle
import nbformat
import pandas as pd
from IPython.display import display
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.commands.alerts import AlertsResultsNormalizer
from taegis_magic.commands.events import TaegisEventQueryNormalizer


logger = logging.getLogger(__name__)


def encode_obj_as_base64_pickle(obj: TaegisResultsNormalizer) -> str:
    """Encode any Taegis Normalized object as a base64 encoded bytes stream.

    Parameters
    ----------
    obj : TaegisResultsNormalizer
        Taegis Normalized Object

    Returns
    -------
    str
        base64 encoded bytes stream
    """
    with BytesIO() as bytes_io:
        compress_pickle.dump(obj, bytes_io, compression="lzma")
        return base64.b64encode(bytes_io.getvalue()).decode()


def decode_base64_obj_as_pickle(b64_string: str) -> TaegisResultsNormalizer:
    """Decode a base64 encoded bytes stream as an Taegis Normalized object.

    Parameters
    ----------
    b64_string : str
        base64 encoded bytes stream

    Returns
    -------
    TaegisResultsNormalizer
        Taegis Normalized object
    """
    return compress_pickle.loads(base64.b64decode(b64_string), compression="lzma")


def read_notebook(path: Union[str, Path]) -> nbformat.NotebookNode:
    """Parse `.ipynb` file into `NotebookNode` object.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook file

    Returns
    -------
    nbformat.NotebookNode
        Notebook
    """
    if isinstance(path, str):
        path = Path(path).resolve()

    if not path.exists():
        raise EnvironmentError(f"{str(path)} does not exist...")

    return nbformat.reads(
        path.read_text(encoding="utf-8"), as_version=nbformat.current_nbformat
    )


def get_cache_list(path: Union[str, Path]) -> List[Tuple[str, str]]:
    """Get a list of cached object in a notebook.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook

    Returns
    -------
    List[Tuple[str, str]]
        List of cached object names
    """
    nb = read_notebook(path)

    return [
        (
            (output.get("metadata", {}) or {}).get("name", ""),
            (output.get("metadata", {}) or {}).get("hash", ""),
        )
        for cell in (nb.cells or [])
        for output in (cell.get("outputs", []) or [])
    ]


def get_cache_item(
    path: Union[str, Path], name: str, cache_source_hash: str
) -> Dict[str, Any]:
    """Get named object from cache.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook
    name : str
        Name of the cached object

    Returns
    -------
    Dict[str, Any]
        Cached object.
    """
    if name not in [item[0] for item in get_cache_list(path)]:
        logger.debug(f"{name} not found in {str(path)} cache...")
        return {}

    nb = read_notebook(path)

    try:
        cache = next(
            iter(
                [
                    (output.get("metadata", {}) or {})
                    for cell in (nb.cells or [])
                    for output in (cell.get("outputs", []) or [])
                    if output.get("metadata", {}).get("name") == name
                    and output.get("metadata", {}).get("hash") == cache_source_hash
                ]
            )
        )
    except Exception as e:
        logger.error(f"Error searching cache::{type(e).__name__}: {e}...")
        cache = {}

    return cache


def get_cached_objects(path: Union[str, Path]) -> List[TaegisResultsNormalizer]:
    """Get a list of all cached objects in a notebook.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook

    Returns
    -------
    List[TaegisResultsNormalizer]
        List of Taegis Normalized objects
    """
    return [
        decode_base64_obj_as_pickle(
            get_cache_item(path=path, name=item[0], cache_source_hash=item[1]).get(
                "data", ""
            )
        )
        for item in get_cache_list(path)
        if item[0]
    ]


def notebook_contains_cached_results(path: Union[str, Path]) -> bool:
    """Check if a notebook contains all null queries.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook

    Returns
    -------
    bool
        Does the notebook contain query results?

    Example
    -------
    Example::

        downloaded_notebooks = list(Path(DOWNLOAD_DIRECTORY).rglob('*.ipynb'))
        notebooks_with_null_findings = [
            notebook
            for notebook in downloaded_notebooks
            if notebook_is_null(notebook)
        ]
    """
    if (
        sum(
            [
                obj.results_returned
                for obj in get_cached_objects(path)
                if isinstance(
                    obj, (AlertsResultsNormalizer, TaegisEventQueryNormalizer)
                )
            ]
        )
        == 0
    ):
        return True

    return False


def display_cache(name: str, cache_digest: str, data: Any):
    """Display data and cache within output.

    Parameters
    ----------
    name : str
        Name of object.
    hash : str
        Unique hash for cache.
    data : Any
        Data to be cached.
    """
    display(
        data,
        metadata={
            "name": name,
            "data": encode_obj_as_base64_pickle(data),
            "hash": cache_digest,
        },
        exclude=["text/plain"],
    )
