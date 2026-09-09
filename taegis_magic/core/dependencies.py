"""Dependency management for Taegis Magic notebooks."""

import os
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import nbformat
import tomli_w

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


log = logging.getLogger(__name__)


def read(
    notebook: str,
    regex: str = r"(?m)^# /// (?P<type>[a-zA-Z0-9-]+)$\s(?P<content>(^#(| .*)$\s)+)^# ///$",
) -> Optional[Dict[str, Any]]:
    """Read the pyproject toml from a notebook."""
    name = "notebook"

    nb = nbformat.read(notebook, as_version=4)

    try:
        cell = next(
            iter(
                cell
                for cell in nb.cells
                if "pyproject" in cell.metadata.get("tags", [])
            )
        )
    except StopIteration:
        log.debug(f'No cell with tag "pyproject" found in {notebook}')
        return None

    matches = list(
        filter(lambda m: m.group("type") == name, re.finditer(regex, cell.source))
    )
    if len(matches) > 1:
        raise ValueError(f"Multiple {name} blocks found")
    elif len(matches) == 1:
        content = "".join(
            line[2:] if line.startswith("# ") else line[1:]
            for line in matches[0].group("content").splitlines(keepends=True)
        )

        try:
            toml_content = tomllib.loads(content)
        except tomllib.TOMLDecodeError as e:
            log.error(f"Error parsing TOML in {notebook}: {e}")
            return None

        return toml_content
    else:
        log.warning(f'No {name} block found in cell with tag "pyproject" in {notebook}')
        return None


def install_dependencies(
    notebook: str,
    virtual_environment: str,
) -> None:
    """Install dependencies declared in a notebook into a virtual environment.

    The environment is created with ``uv venv`` when its Python interpreter does
    not exist. Dependencies are installed with ``uv pip`` so the notebook can
    be executed using the environment's interpreter.
    """
    document = read(notebook)
    if not document:
        return

    dependencies = document.get("dependencies", [])
    if not dependencies:
        return
    if not isinstance(dependencies, list) or not all(
        isinstance(dependency, str) for dependency in dependencies
    ):
        raise ValueError("Notebook dependencies must be a list of strings")

    environment_path = Path(virtual_environment)
    python_name = "python.exe" if sys.platform == "win32" else "python"
    python_path = environment_path / (
        "Scripts" if sys.platform == "win32" else "bin"
    ) / python_name

    if not python_path.exists():
        subprocess.run(
            ["uv", "venv", str(environment_path)],
            check=True,
        )

    subprocess.run(
        ["uv", "run", "install", "--python", str(python_path), '--upgrade', *dependencies],
        check=True,
    )


def clean_ansi_format(s: str) -> str:
    # Regex pattern to match ANSI escape sequences
    ansi_pattern = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
    return ansi_pattern.sub("", s)


def _build_dep_list() -> list[str]:
    """
    Build a list of dependencies from the notebook.
    """
    sp = subprocess.run(
        ["uv", "pip", "freeze", "--exclude-editable"],
        check=True,
        capture_output=True,
        encoding="utf-8",
    )

    stdout = clean_ansi_format(sp.stdout)

    return [f'"{line}"' for line in stdout.splitlines()]


def generate_pyproject() -> str:
    """
    Generate a pyproject cell file from the dependencies in the notebook.
    """
    python_version = f"{sys.version_info.major}.{sys.version_info.minor}"

    document = {}
    document["requires-python"] = f">={python_version}"
    document["dependencies"] = _build_dep_list()
    content = tomli_w.dumps(document)

    pyproject = f"/// notebook\n{content}\n///\n"

    pyproject = "\n".join([f"# {line}" for line in pyproject.splitlines()])

    return pyproject


def add_pyproject_cell(notebook_path: str, pyproject_content: str) -> None:
    """
    Add a pyproject cell to the notebook.
    """
    nb = nbformat.read(notebook_path, as_version=4)

    # Create a new code cell with the pyproject content
    new_cell = nbformat.v4.new_code_cell(source=pyproject_content)
    new_cell.metadata.tags = ["pyproject"]

    found = False
    for cell in nb.cells:
        print(cell)
        if "pyproject" in cell.metadata.tags:
            log.info(f"Overwriting existing pyproject cell from {notebook_path}")
            cell = new_cell
            found = True

    if not found:
        nb.cells = [new_cell] + nb.cells

    # Write the updated notebook back to the file
    with open(notebook_path, "w", encoding="utf-8") as f:
        nbformat.write(nb, f)
