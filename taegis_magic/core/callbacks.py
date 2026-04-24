"""Taegis Magic Command callback utilities."""

from pathlib import Path

import logging
import typer

log = logging.getLogger(__name__)


def verify_file(value: str):
    """Verify a file on disk."""
    fp = Path(value)
    overwrite = None
    options = ["y", "n"]

    log.debug(f"Checking if file {value} exists.")
    if fp.exists():
        while not overwrite in options:
            overwrite = input(f"{fp} exists. Overwrite [{'/'.join(options)}]? ")

        if overwrite == "n":
            raise typer.Exit()

    return fp
