"""Dependency management for Taegis Magic notebooks."""

import logging
import re
import subprocess
import sys
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
            iter(cell for cell in nb.cells if "pyproject" in cell.metadata.tags)
        )
    except StopIteration:
        log.error(f'No cell with tag "pyproject" found in {notebook}')
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
        return tomllib.loads(content)
    else:
        log.warning(f'No {name} block found in cell with tag "pyproject" in {notebook}')
        return None


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
