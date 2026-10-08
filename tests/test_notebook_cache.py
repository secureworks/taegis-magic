from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import nbformat
import pytest
import typer

from taegis_magic.commands import notebook as notebook_commands
from taegis_magic.core import cache as cache_module
from taegis_magic.core.cache import (
    CACHE_KIND_VARIABLE,
    VariableCacheClear,
    clear_live_variable_cache_outputs,
    clear_variable_cache,
    decode_base64_obj_as_pickle,
    delete_variable_cache_entry,
    display_variable_cache,
    encode_obj_as_base64_pickle,
    get_session_variable_cache_hashes,
    get_variable_cache_item,
    get_variable_cache_list,
    get_variable_cache_preview,
    variable_cache_display_id,
)
from taegis_magic.core.notebook import TAEGIS_MAGIC_NOTEBOOK_FILENAME, save_notebook


@pytest.fixture(autouse=True)
def live_update():
    """Isolate the session dump registry and capture live display updates."""
    cache_module._session_dumps.clear()
    with patch.object(cache_module, "update_display") as mock_update:
        yield mock_update
    cache_module._session_dumps.clear()


def live_cleared_ids(mock_update) -> List[str]:
    return [c.kwargs["display_id"] for c in mock_update.call_args_list]


def variable_output(name: str, value: Any, hash_: str = "h") -> nbformat.NotebookNode:
    return nbformat.v4.new_output(
        output_type="display_data",
        data={"text/markdown": name},
        metadata={
            "name": name,
            "data": encode_obj_as_base64_pickle(value),
            "hash": hash_,
            "kind": CACHE_KIND_VARIABLE,
            "type": type(value).__name__,
        },
    )


def query_output(name: str, hash_: str = "q") -> nbformat.NotebookNode:
    return nbformat.v4.new_output(
        output_type="display_data",
        data={"text/markdown": name},
        metadata={
            "name": name,
            "data": encode_obj_as_base64_pickle({"results": []}),
            "hash": hash_,
        },
    )


def stream_output(text: str) -> nbformat.NotebookNode:
    return nbformat.v4.new_output(output_type="stream", name="stdout", text=text)


def write_cells(
    tmp_path: Path, cells: List[Tuple[str, List[nbformat.NotebookNode]]]
) -> Path:
    nb = nbformat.v4.new_notebook()
    for source, outputs in cells:
        cell = nbformat.v4.new_code_cell(source=source)
        cell.outputs = outputs
        nb.cells.append(cell)
    nb.cells.append(nbformat.v4.new_markdown_cell(source="notes"))

    path = tmp_path / "notebook.ipynb"
    nbformat.write(nb, str(path))
    return path


def write_notebook(
    tmp_path: Path, cells_outputs: List[List[nbformat.NotebookNode]]
) -> Path:
    return write_cells(tmp_path, [("", outputs) for outputs in cells_outputs])


def cell_outputs(path: Path) -> List[List[str]]:
    nb = nbformat.read(str(path), as_version=nbformat.current_nbformat)
    return [
        [
            output.get("text") or output.get("metadata", {}).get("name")
            for output in cell.outputs
        ]
        for cell in nb.cells
        if cell.cell_type == "code"
    ]


def output_names(path: Path, kind: Optional[str] = CACHE_KIND_VARIABLE) -> List[str]:
    nb = nbformat.read(str(path), as_version=nbformat.current_nbformat)
    return [
        output.metadata.get("name")
        for cell in nb.cells
        for output in cell.get("outputs", [])
        if output.metadata.get("kind") == kind
    ]


def test_display_variable_cache_metadata():
    value = {"a": 1}
    with patch("taegis_magic.core.cache.display") as mock_display:
        display_variable_cache("my_var", "digest", value)

    mock_display.assert_called_once()
    assert mock_display.call_args.kwargs["raw"] is True
    assert "my_var" in mock_display.call_args.args[0]["text/markdown"]
    metadata: Dict[str, Any] = mock_display.call_args.kwargs["metadata"]
    assert metadata["name"] == "my_var"
    assert metadata["hash"] == "digest"
    assert metadata["kind"] == "variable"
    assert metadata["type"] == "dict"
    assert decode_base64_obj_as_pickle(metadata["data"]) == value
    assert mock_display.call_args.kwargs["display_id"] == "taegis-cache-digest"
    assert get_session_variable_cache_hashes() == ["digest"]
    assert get_session_variable_cache_hashes("other") == []


def test_clear_live_variable_cache_outputs(live_update):
    cache_module._session_dumps.update({"h1": "a", "h2": "b"})

    clear_live_variable_cache_outputs(["h1", "h1", "h3"])

    assert live_cleared_ids(live_update) == [
        variable_cache_display_id("h1"),
        variable_cache_display_id("h3"),
    ]
    for c in live_update.call_args_list:
        assert c.args == ({"text/markdown": ""},)
        assert c.kwargs["raw"] is True
        assert c.kwargs["metadata"] == {}
    assert get_session_variable_cache_hashes() == ["h2"]


def test_get_variable_cache_list_ignores_query_cache(tmp_path):
    path = write_notebook(
        tmp_path,
        [
            [query_output("alerts")],
            [variable_output("a", 1, "h1")],
            [variable_output("b", "x", "h2"), query_output("events")],
        ],
    )

    assert get_variable_cache_list(path) == [("a", "h1"), ("b", "h2")]


def test_get_variable_cache_item_latest_wins(tmp_path):
    path = write_notebook(
        tmp_path,
        [[variable_output("a", 1)], [query_output("a")], [variable_output("a", 2)]],
    )

    item = get_variable_cache_item(path, "a")
    assert decode_base64_obj_as_pickle(item["data"]) == 2


def test_get_variable_cache_item_ignores_query_cache(tmp_path):
    path = write_notebook(tmp_path, [[query_output("alerts")]])

    assert get_variable_cache_item(path, "alerts") == {}


def test_get_variable_cache_preview_short_value(tmp_path):
    path = write_notebook(tmp_path, [[variable_output("a", [1, 2])]])

    assert get_variable_cache_preview(path, "a") == ("list", "[1, 2]")


def test_get_variable_cache_preview_truncates_long_value(tmp_path):
    path = write_notebook(tmp_path, [[variable_output("a", list(range(100)))]])

    type_name, preview = get_variable_cache_preview(path, "a")
    assert type_name == "list"
    assert len(preview) == 80
    assert preview.endswith("…")
    assert preview.startswith("[0, 1, 2")


def test_get_variable_cache_preview_missing_name(tmp_path):
    path = write_notebook(tmp_path, [[variable_output("a", 1)]])

    assert get_variable_cache_preview(path, "missing") is None


def test_get_variable_cache_preview_type_from_metadata_without_decode(tmp_path):
    output = variable_output("a", 1)
    output.metadata["data"] = "not-a-valid-payload"
    output.metadata["type"] = "CustomType"
    path = write_notebook(tmp_path, [[output]])

    type_name, preview = get_variable_cache_preview(path, "a")
    assert type_name == "CustomType"
    assert preview.startswith("<unable to decode")


def test_delete_variable_cache_entry(tmp_path):
    path = write_notebook(
        tmp_path,
        [
            [variable_output("a", 1, "h1"), query_output("a")],
            [variable_output("b", 2, "h2")],
            [variable_output("a", 3, "h3")],
        ],
    )

    assert delete_variable_cache_entry(path, "a") == ["h1", "h3"]
    assert output_names(path) == ["b"]
    assert output_names(path, kind=None) == ["a"]


def test_delete_variable_cache_entry_missing_name_leaves_file(tmp_path):
    path = write_notebook(tmp_path, [[variable_output("a", 1), query_output("q")]])
    before = path.read_bytes()

    assert delete_variable_cache_entry(path, "missing") == []
    assert path.read_bytes() == before


def test_delete_variable_cache_entry_does_not_match_query_cache(tmp_path):
    path = write_notebook(tmp_path, [[query_output("alerts")]])
    before = path.read_bytes()

    assert delete_variable_cache_entry(path, "alerts") == []
    assert path.read_bytes() == before


def test_clear_variable_cache(tmp_path):
    path = write_notebook(
        tmp_path,
        [
            [variable_output("a", 1, "h1"), query_output("alerts")],
            [query_output("events")],
            [variable_output("b", 2, "h2"), variable_output("c", 3, "h3")],
        ],
    )

    assert clear_variable_cache(path) == VariableCacheClear(["h1", "h2", "h3"], 2, 0)
    assert output_names(path) == []
    assert output_names(path, kind=None) == ["alerts", "events"]


def test_clear_variable_cache_empty_leaves_file(tmp_path):
    path = write_notebook(tmp_path, [[query_output("alerts")]])
    before = path.read_bytes()

    assert clear_variable_cache(path) == VariableCacheClear()
    assert path.read_bytes() == before


def test_clear_removes_all_output_of_dump_cells(tmp_path):
    path = write_cells(
        tmp_path,
        [
            ("print('hi')", [stream_output("hi\n")]),
            (
                "%taegis notebook cache dump a",
                [
                    variable_output("a", 1),
                    stream_output("saving\n"),
                    stream_output("summary\n"),
                ],
            ),
        ],
    )

    assert clear_variable_cache(path) == VariableCacheClear(["h"], 1, 2)
    assert cell_outputs(path) == [["hi\n"], []]


def test_clear_clears_dump_cell_whose_entry_was_deleted(tmp_path):
    path = write_cells(
        tmp_path,
        [("x = 1\n%taegis notebook cache dump x", [stream_output("summary\n")])],
    )

    assert clear_variable_cache(path) == VariableCacheClear([], 1, 1)
    assert cell_outputs(path) == [[]]


def test_clear_keeps_query_cache_in_dump_cell(tmp_path):
    path = write_cells(
        tmp_path,
        [
            (
                "%taegis alerts search --cache --assign alerts\n%taegis notebook cache dump a",
                [
                    query_output("alerts"),
                    variable_output("a", 1),
                    stream_output("summary\n"),
                ],
            )
        ],
    )

    assert clear_variable_cache(path) == VariableCacheClear(["h"], 1, 1)
    assert cell_outputs(path) == [["alerts"]]


@pytest.mark.parametrize(
    "source",
    [
        "%taegis notebook cache list",
        "%taegis notebook cache load a",
        "# notebook cache dump",
    ],
)
def test_clear_ignores_other_cells(tmp_path, source):
    path = write_cells(tmp_path, [(source, [stream_output("out\n")])])
    before = path.read_bytes()

    assert clear_variable_cache(path) == VariableCacheClear()
    assert path.read_bytes() == before


@pytest.mark.parametrize("path_type", [str, Path])
def test_remove_accepts_str_and_path(tmp_path, path_type):
    path = write_notebook(tmp_path, [[variable_output("a", 1)]])

    assert clear_variable_cache(path_type(path)) == VariableCacheClear(["h"], 1, 0)


@pytest.fixture
def user_ns():
    ns: Dict[str, Any] = {}
    shell = MagicMock()
    shell.user_ns = ns
    with patch.object(notebook_commands, "get_ipython", return_value=shell):
        yield ns


@pytest.fixture
def notebook_ns(tmp_path, user_ns):
    def _make(cells_outputs: List[List[nbformat.NotebookNode]]) -> Path:
        path = write_notebook(tmp_path, cells_outputs)
        user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME] = str(path)
        return path

    return _make


def test_dump_caches_variable(user_ns):
    user_ns["my_var"] = {"a": 1}

    with (
        patch.object(notebook_commands, "display_variable_cache") as mock_cache,
        patch.object(notebook_commands, "save_notebook") as mock_save,
    ):
        result = notebook_commands.dump("my_var")

    name, _digest, value = mock_cache.call_args.args
    assert (name, value) == ("my_var", {"a": 1})
    mock_save.assert_called_once_with(quiet=True)
    assert result.raw_results == notebook_commands.CacheDumpResult("my_var", "dict")
    assert getattr(result, "hide_display", False) is True


def test_dump_missing_variable(user_ns, capsys):
    with (
        patch.object(notebook_commands, "display_variable_cache") as mock_cache,
        patch.object(notebook_commands, "save_notebook") as mock_save,
        pytest.raises(typer.Exit),
    ):
        notebook_commands.dump("missing")

    mock_cache.assert_not_called()
    mock_save.assert_not_called()
    assert "not found" in capsys.readouterr().out


def test_dump_without_kernel(capsys):
    with (
        patch.object(notebook_commands, "get_ipython", return_value=None),
        patch.object(notebook_commands, "display_variable_cache") as mock_cache,
        pytest.raises(typer.Exit),
    ):
        notebook_commands.dump("my_var")

    mock_cache.assert_not_called()
    assert "live notebook session" in capsys.readouterr().out


def test_load_sets_user_ns(notebook_ns, user_ns):
    notebook_ns([[variable_output("my_var", [1, 2])], [variable_output("my_var", [3])]])

    result = notebook_commands.load("my_var")

    assert user_ns["my_var"] == [3]
    assert result.raw_results == notebook_commands.CacheLoadResult("my_var", "list")


def test_load_missing_name(notebook_ns, user_ns):
    notebook_ns([[query_output("my_var")]])

    with pytest.raises(typer.Exit):
        notebook_commands.load("my_var")

    assert "my_var" not in user_ns


def test_load_without_notebook_filename(user_ns, capsys):
    with pytest.raises(typer.Exit):
        notebook_commands.load("my_var")

    assert TAEGIS_MAGIC_NOTEBOOK_FILENAME in capsys.readouterr().out


def test_load_notebook_not_on_disk(tmp_path, user_ns, capsys):
    user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME] = str(tmp_path / "missing.ipynb")

    with pytest.raises(typer.Exit):
        notebook_commands.load("my_var")

    assert "does not exist" in capsys.readouterr().out


def test_delete_removes_entry(notebook_ns, capsys):
    path = notebook_ns(
        [[variable_output("a", 1), query_output("q")], [variable_output("b", 2)]]
    )

    result = notebook_commands.delete("a")

    assert output_names(path) == ["b"]
    assert output_names(path, kind=None) == ["q"]
    assert result.raw_results == notebook_commands.CacheDeleteResult("a", True)
    # Dumped in an earlier session: the open notebook can't be updated, so warn.
    assert "reload" in capsys.readouterr().out


def test_delete_clears_entry_dumped_this_session(notebook_ns, live_update, capsys):
    path = notebook_ns([[variable_output("a", 1, "h1")], [variable_output("b", 2)]])
    cache_module._session_dumps["h1"] = "a"

    notebook_commands.delete("a")

    assert output_names(path) == ["b"]
    assert live_cleared_ids(live_update) == [variable_cache_display_id("h1")]
    out = capsys.readouterr().out
    assert "Removed 'a'" in out
    assert "reload" not in out


def test_delete_unsaved_session_dump(notebook_ns, live_update, capsys):
    path = notebook_ns([[query_output("q")]])
    before = path.read_bytes()
    cache_module._session_dumps["h1"] = "a"

    result = notebook_commands.delete("a")

    assert path.read_bytes() == before
    assert live_cleared_ids(live_update) == [variable_cache_display_id("h1")]
    assert result.raw_results == notebook_commands.CacheDeleteResult("a", True)
    assert "reload" not in capsys.readouterr().out


def test_delete_missing_name(notebook_ns, live_update):
    path = notebook_ns([[variable_output("a", 1)]])
    before = path.read_bytes()

    with pytest.raises(typer.Exit):
        notebook_commands.delete("missing")

    assert path.read_bytes() == before
    live_update.assert_not_called()


def test_delete_does_not_save_notebook(notebook_ns):
    notebook_ns([[variable_output("a", 1)]])

    with patch.object(notebook_commands, "save_notebook") as mock_save:
        notebook_commands.delete("a")
        notebook_commands.clear()

    mock_save.assert_not_called()


def test_clear_removes_all_entries(notebook_ns, capsys):
    path = notebook_ns(
        [
            [variable_output("a", 1, "h1"), query_output("q")],
            [variable_output("b", 2, "h2")],
        ]
    )

    result = notebook_commands.clear()

    assert output_names(path) == []
    assert output_names(path, kind=None) == ["q"]
    assert result.raw_results == notebook_commands.CacheClearResult(2, 2)
    # Dumped in an earlier session: the open notebook can't be updated, so warn.
    assert "reload" in capsys.readouterr().out


def test_clear_session_dumps_without_warning(tmp_path, user_ns, live_update, capsys):
    path = write_cells(
        tmp_path,
        [
            ("%taegis notebook cache dump a", [variable_output("a", 1, "h1")]),
            ("print('hi')", [stream_output("hi\n")]),
        ],
    )
    user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME] = str(path)
    cache_module._session_dumps.update({"h1": "a", "h2": "b"})

    result = notebook_commands.clear()

    assert cell_outputs(path) == [[], ["hi\n"]]
    assert live_cleared_ids(live_update) == [
        variable_cache_display_id("h1"),
        variable_cache_display_id("h2"),
    ]
    assert result.raw_results == notebook_commands.CacheClearResult(2, 1)
    assert get_session_variable_cache_hashes() == []
    assert "reload" not in capsys.readouterr().out


def test_clear_warns_when_other_dump_output_removed(tmp_path, user_ns, capsys):
    path = write_cells(
        tmp_path,
        [
            (
                "%taegis notebook cache dump a",
                [variable_output("a", 1, "h1"), stream_output("summary\n")],
            )
        ],
    )
    user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME] = str(path)
    cache_module._session_dumps["h1"] = "a"

    notebook_commands.clear()

    assert "reload" in capsys.readouterr().out


def test_clear_empty_is_not_error(notebook_ns, live_update, capsys):
    path = notebook_ns([[query_output("q")]])
    before = path.read_bytes()

    result = notebook_commands.clear()

    assert result.raw_results == notebook_commands.CacheClearResult(0, 0)
    assert path.read_bytes() == before
    assert "nothing to clear" in capsys.readouterr().out
    live_update.assert_not_called()


def test_list_shows_variable_entries(notebook_ns, capsys):
    notebook_ns(
        [
            [variable_output("a", 1), query_output("alerts")],
            [variable_output("b", "x" * 200)],
            [variable_output("a", 2.5)],
        ]
    )

    result = notebook_commands.list_cache()

    entries = result.raw_results.entries
    assert [(e.name, e.type_name) for e in entries] == [("a", "float"), ("b", "str")]
    assert entries[0].preview == "2.5"
    assert len(entries[1].preview) == 80

    out = capsys.readouterr().out
    assert "float" in out and "str" in out
    assert "alerts" not in out


def test_list_type_recorded_at_dump_time(notebook_ns, user_ns):
    notebook_ns([[variable_output("a", 1)]])
    user_ns["a"] = "now a string"

    result = notebook_commands.list_cache()

    assert result.raw_results.entries[0].type_name == "int"


def test_list_empty(notebook_ns, capsys):
    notebook_ns([[query_output("alerts")]])

    result = notebook_commands.list_cache()

    assert result.raw_results.entries == []
    assert "No cached variable entries" in capsys.readouterr().out


def test_dump_list_load_delete_lifecycle(tmp_path, notebook_ns, user_ns):
    path = notebook_ns([[query_output("alerts")]])
    user_ns["frame"] = {"rows": [1, 2, 3]}

    published = []
    with (
        patch(
            "taegis_magic.core.cache.display",
            side_effect=lambda data, raw, metadata, display_id: published.append(
                nbformat.v4.new_output(
                    output_type="display_data", data=data, metadata=metadata
                )
            ),
        ),
        patch.object(notebook_commands, "save_notebook"),
    ):
        notebook_commands.dump("frame")

    # Simulate the frontend saving the dumped output into the notebook file.
    nb = nbformat.read(str(path), as_version=nbformat.current_nbformat)
    cell = nbformat.v4.new_code_cell(source="%taegis notebook cache dump frame")
    cell.outputs = published
    nb.cells.append(cell)
    nbformat.write(nb, str(path))

    del user_ns["frame"]
    assert [e.name for e in notebook_commands.list_cache().raw_results.entries] == [
        "frame"
    ]

    notebook_commands.load("frame")
    assert user_ns["frame"] == {"rows": [1, 2, 3]}

    notebook_commands.delete("frame")
    assert notebook_commands.list_cache().raw_results.entries == []
    assert output_names(path, kind=None) == ["alerts"]


@pytest.mark.parametrize("quiet, level", [(False, "error"), (True, "debug")])
def test_save_notebook_quiet_in_vscode(quiet, level):
    shell = MagicMock()
    shell.user_ns = {"__vsc_ipynb_file__": "nb.ipynb"}
    with (
        patch("taegis_magic.core.notebook.get_ipython", return_value=shell),
        patch("taegis_magic.core.notebook.log") as mock_log,
    ):
        save_notebook(quiet=quiet)

    called = [name for name in ("debug", "error") if getattr(mock_log, name).called]
    assert called == [level]
