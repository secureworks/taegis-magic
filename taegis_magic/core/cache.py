"""Cache data within IPython output cells."""

import base64
import logging
import re
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import compress_pickle
import nbformat
import pandas as pd
from IPython.display import display, update_display

from taegis_magic.commands.alerts import AlertsResultsNormalizer
from taegis_magic.commands.events import TaegisEventQueryNormalizer
from taegis_magic.core.normalizer import TaegisResultsNormalizer

log = logging.getLogger(__name__)

CACHE_KIND_VARIABLE = "variable"
PREVIEW_MAX_LENGTH = 80

# Variable-cache entries dumped in this kernel session: hash -> name.
_session_dumps: Dict[str, str] = {}
DUMP_COMMAND_PATTERN = re.compile(
    r"^\s*%{1,2}taegis\b.*\bnotebook\s+cache\s+dump\b", re.MULTILINE
)


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
        log.info(f"{name} not found in {str(path)} cache...")
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
        log.exception(f"Error searching cache::{type(e).__name__}: {e}...")
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


def variable_cache_display_id(cache_digest: str) -> str:
    """Display id used to update a variable-cache output in the running notebook."""
    return f"taegis-cache-{cache_digest}"


def display_variable_cache(name: str, cache_digest: str, data: Any):
    """Display a user variable and cache it within output as a variable-cache entry.

    The output is published with a display id and recorded for this kernel
    session, so it can later be cleared from the running notebook.

    Parameters
    ----------
    name : str
        Name of the variable.
    cache_digest : str
        Unique hash for this cache entry.
    data : Any
        Variable value to be cached.
    """
    type_name = type(data).__name__
    # Published as a raw bundle: most objects only have a text/plain repr, which
    # display() would otherwise drop entirely when text/plain is excluded.
    display(
        {"text/markdown": f"Cached variable `{name}` (`{type_name}`)"},
        raw=True,
        metadata={
            "name": name,
            "data": encode_obj_as_base64_pickle(data),
            "hash": cache_digest,
            "kind": CACHE_KIND_VARIABLE,
            "type": type_name,
        },
        display_id=variable_cache_display_id(cache_digest),
    )
    _session_dumps[cache_digest] = name


def get_session_variable_cache_hashes(name: Optional[str] = None) -> List[str]:
    """Get hashes of variable-cache entries dumped in this kernel session.

    Parameters
    ----------
    name : Optional[str]
        Only return entries for this variable name, by default all entries

    Returns
    -------
    List[str]
        Cache entry hashes
    """
    return [
        cache_digest
        for cache_digest, dumped_name in _session_dumps.items()
        if name is None or dumped_name == name
    ]


def clear_live_variable_cache_outputs(cache_digests: List[str]):
    """Clear variable-cache outputs from the running notebook by display id.

    Replaces each output's data and metadata with empty values, so the open
    notebook stops showing it and no longer saves the cache entry. Frontends
    only know display ids for outputs shown since the notebook was opened;
    other ids are ignored.

    Parameters
    ----------
    cache_digests : List[str]
        Hashes of the cache entries to clear
    """
    for cache_digest in dict.fromkeys(cache_digests):
        # Same single mime type as the dump bundle: VS Code only replaces an
        # output's metadata when the update has the same number of mime items,
        # otherwise it swaps the items and keeps the old cache metadata.
        update_display(
            {"text/markdown": ""},
            raw=True,
            metadata={},
            display_id=variable_cache_display_id(cache_digest),
        )
        _session_dumps.pop(cache_digest, None)


def _variable_cache_outputs(nb: nbformat.NotebookNode) -> List[Dict[str, Any]]:
    return [
        output.get("metadata", {}) or {}
        for cell in (nb.cells or [])
        for output in (cell.get("outputs", []) or [])
        if (output.get("metadata", {}) or {}).get("kind") == CACHE_KIND_VARIABLE
    ]


def get_variable_cache_list(path: Union[str, Path]) -> List[Tuple[str, str]]:
    """Get (name, hash) for each variable-cache entry in a notebook.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook

    Returns
    -------
    List[Tuple[str, str]]
        Variable-cache entry names and hashes, in notebook order
    """
    return [
        (metadata.get("name", ""), metadata.get("hash", ""))
        for metadata in _variable_cache_outputs(read_notebook(path))
    ]


def get_variable_cache_item(path: Union[str, Path], name: str) -> Dict[str, Any]:
    """Get the most recent variable-cache entry for a name.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook
    name : str
        Name of the cached variable

    Returns
    -------
    Dict[str, Any]
        Cache entry metadata, or an empty dict if not found
    """
    matches = [
        metadata
        for metadata in _variable_cache_outputs(read_notebook(path))
        if metadata.get("name") == name
    ]
    if not matches:
        log.info(f"{name} not found in {path} variable cache...")
        return {}

    return matches[-1]


def _truncate(text: str, max_length: int = PREVIEW_MAX_LENGTH) -> str:
    if len(text) > max_length:
        return text[: max_length - 1] + "…"
    return text


def get_variable_cache_preview(
    path: Union[str, Path], name: str
) -> Optional[Tuple[str, str]]:
    """Get the recorded type and a truncated value preview for a cached variable.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook
    name : str
        Name of the cached variable

    Returns
    -------
    Optional[Tuple[str, str]]
        (type name, truncated repr), or None if not found
    """
    item = get_variable_cache_item(path, name)
    if not item:
        return None

    try:
        preview = repr(decode_base64_obj_as_pickle(item.get("data", "")))
    except Exception as e:  # noqa: BLE001 - unpickling can raise any exception type
        log.error(f"Unable to decode cached variable {name}: {type(e).__name__}: {e}")
        preview = f"<unable to decode: {type(e).__name__}>"

    return item.get("type", ""), _truncate(preview)


def _is_variable_cache_output(output: Dict[str, Any]) -> bool:
    return (output.get("metadata", {}) or {}).get("kind") == CACHE_KIND_VARIABLE


def _is_query_cache_output(output: Dict[str, Any]) -> bool:
    metadata = output.get("metadata", {}) or {}
    return "data" in metadata and metadata.get("kind") != CACHE_KIND_VARIABLE


def _is_cache_dump_cell(cell: Dict[str, Any]) -> bool:
    return bool(DUMP_COMMAND_PATTERN.search(cell.get("source", "") or "")) or any(
        _is_variable_cache_output(output) for output in cell.get("outputs", []) or []
    )


def delete_variable_cache_entry(path: Union[str, Path], name: str) -> List[str]:
    """Remove all variable-cache entries for a name from the notebook file on disk.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook
    name : str
        Name of the cached variable

    Returns
    -------
    List[str]
        Hashes of the removed entries, empty if nothing was removed
    """
    if isinstance(path, str):
        path = Path(path).resolve()

    nb = read_notebook(path)
    removed: List[str] = []

    for cell in nb.cells or []:
        if "outputs" not in cell:
            continue

        kept = []
        for output in cell.get("outputs", []) or []:
            metadata = output.get("metadata", {}) or {}
            if _is_variable_cache_output(output) and metadata.get("name") == name:
                removed.append(metadata.get("hash", ""))
            else:
                kept.append(output)
        cell["outputs"] = kept

    if removed:
        nbformat.write(nb, str(path))

    return removed


@dataclass
class VariableCacheClear:
    """Outcome of clearing the variable cache from a notebook file."""

    removed_hashes: List[str] = field(default_factory=list)
    cleared_cells: int = 0
    other_outputs_removed: int = 0


def clear_variable_cache(path: Union[str, Path]) -> VariableCacheClear:
    """Remove every variable-cache entry and clear the output of cache dump cells.

    A cache dump cell is any cell holding a variable-cache entry or running
    `%taegis notebook cache dump`. All of its outputs are removed except
    query-result cache entries written by `--cache`.

    Parameters
    ----------
    path : Union[str, Path]
        Path to notebook

    Returns
    -------
    VariableCacheClear
        Removed entry hashes, dump cells cleared, and other outputs removed
    """
    if isinstance(path, str):
        path = Path(path).resolve()

    nb = read_notebook(path)
    outcome = VariableCacheClear()

    for cell in nb.cells or []:
        if "outputs" not in cell or not _is_cache_dump_cell(cell):
            continue

        outputs = cell.get("outputs", []) or []
        kept = []
        for output in outputs:
            if _is_query_cache_output(output):
                kept.append(output)
            elif _is_variable_cache_output(output):
                outcome.removed_hashes.append(output["metadata"].get("hash", ""))
            else:
                outcome.other_outputs_removed += 1
        if len(kept) != len(outputs):
            outcome.cleared_cells += 1
        cell["outputs"] = kept

    if outcome.cleared_cells:
        nbformat.write(nb, str(path))

    return outcome
