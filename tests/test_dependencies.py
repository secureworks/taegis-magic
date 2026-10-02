from unittest.mock import patch

from taegis_magic.core.dependencies import install_dependencies


def test_install_dependencies_creates_environment_and_installs(tmp_path):
    notebook = tmp_path / "notebook.ipynb"
    notebook.write_text(
        '{"cells": [{"cell_type": "code", "id": "dependencies", "metadata": '
        '{"tags": ["pyproject"]}, "source": '
        '["# /// notebook\\n", "# dependencies = [\\\"pandas\\\"]\\n", '
        '"# ///\\n"]}], "metadata": {}, "nbformat": 4, '
        '"nbformat_minor": 5}'
    )
    environment = tmp_path / ".venv"

    with patch("taegis_magic.core.dependencies.subprocess.run") as run:
        install_dependencies(str(notebook), str(environment))

    assert run.call_count == 2
    assert run.call_args_list[0].args[0] == ["uv", "venv", str(environment)]
    assert run.call_args_list[1].args[0] == [
        "uv",
        "pip",
        "install",
        "--python",
        str(environment / "bin" / "python"),
        "--upgrade",
        "pandas",
    ]


def test_install_dependencies_skips_notebook_without_metadata(tmp_path):
    notebook = tmp_path / "notebook.ipynb"
    notebook.write_text(
        '{"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}'
    )

    with patch("taegis_magic.core.dependencies.subprocess.run") as run:
        install_dependencies(str(notebook), str(tmp_path / ".venv"))

    run.assert_not_called()