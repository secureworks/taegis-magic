"""Taegis IPython Magics."""
import hashlib
import logging
import shlex
from argparse import ArgumentError, ArgumentParser
from typing import Optional
from pathlib import Path
from time import sleep
import ipynbname

import pandas as pd
from gql.transport.exceptions import TransportQueryError
from IPython.core.magic import Magics, line_cell_magic, magics_class
from IPython.display import display, display_markdown, Javascript
from taegis_magic.cli import app
from taegis_magic.core.cache import (
    get_cache_item,
    decode_base64_obj_as_pickle,
    display_cache,
)

log = logging.getLogger(__name__)


def taegis_magics_command_parser() -> ArgumentParser:
    """Magics support flags."""
    parser = ArgumentParser("taegis_magic_parser")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--assign",
        metavar="NAME",
        type=str,
        help="Assign results as pandas DataFrame to NAME",
    )
    group.add_argument(
        "--append",
        metavar="NAME",
        type=str,
        help="Append results as pandas DataFrame to NAME",
    )
    parser.add_argument(
        "--display",
        metavar="NAME",
        help="Display NAME as markdown table",
    )
    parser.add_argument(
        "--cache",
        action="store_true",
        help="Save output to cache / Load output from cache (if present)",
    )

    return parser


@magics_class
class TaegisMagics(Magics):
    """Taegis Magics Class."""

    def __init__(self, shell=None, **kwargs):
        super().__init__(shell, **kwargs)

        try:
            notebook_name = ipynbname.path().name
        except FileNotFoundError:
            notebook_name = None

        self.shell.user_ns["TAEGIS_MAGIC_NOTEBOOK_FILENAME"] = notebook_name

    @line_cell_magic
    def taegis(self, line: str, cell: Optional[str] = None):
        """Taegis Magics Line/Cell magic."""
        magic_args = None
        command_args = None
        notebook_filename = None

        args = shlex.split(line)
        parser = taegis_magics_command_parser()

        try:
            magic_args, command_args = parser.parse_known_args(args)
        except SystemExit:
            if "--help" in args or "-h" in args:
                command_args = args
            else:
                return
        except ArgumentError as e:
            print(e)
            return

        log.debug(f"Magic Args: {magic_args}")
        log.debug(f"Command Args: {command_args}")

        if magic_args and magic_args.cache:
            if not magic_args.assign:
                raise ValueError("--assign must be set with cache...")

            if (
                "TAEGIS_MAGIC_NOTEBOOK_FILENAME" in self.shell.user_ns
                and self.shell.user_ns["TAEGIS_MAGIC_NOTEBOOK_FILENAME"]
            ):
                notebook_filename = self.shell.user_ns["TAEGIS_MAGIC_NOTEBOOK_FILENAME"]
            else:
                notebook_filename = input("Notebook Filename:")

            if not notebook_filename:
                raise ValueError("Cannot determine file name of notebook...")

            notebook_fp = Path(notebook_filename)

            if notebook_fp.exists():
                self.shell.user_ns["TAEGIS_MAGIC_NOTEBOOK_FILENAME"] = notebook_filename
            else:
                raise ValueError(
                    "Notebook does not exist on disk, save notebook to disk before caching or "
                    "reload the taegis_magic extension (%reload_ext taegis_magic)..."
                )

            if not cell:
                cell = ""

            cache_digest = hashlib.sha256(bytes(line + cell, "utf-8")).hexdigest()
            cache = get_cache_item(notebook_fp, magic_args.assign, cache_digest)
            if cache:
                log.info(f"{magic_args.assign} found in cache...")
                log.debug("normalizing results...")
                data = decode_base64_obj_as_pickle(cache.get("data"))

                log.debug("converting to dataframe...")
                self.shell.user_ns[magic_args.assign] = pd.json_normalize(
                    data.results, max_level=3
                ).dropna(axis=1, how="all")

                log.info(f"re-setting {magic_args.assign}:{cache_digest} to cache...")
                display_cache(magic_args.assign, cache_digest, data)
                display(Javascript("IPython.notebook.save_checkpoint();"), exclude=["text/plain"])

                return

            log.info(f"{magic_args.assign} not found in cache...")

        if cell:
            command_args.extend(["--cell", cell])

        # os.environ["TAEGIS_MAGIC_OUTPUT"] = "True"
        try:
            result = app(command_args, prog_name="taegis", standalone_mode=False)
        except (SystemExit, TransportQueryError):
            result = None
        # os.environ["TAEGIS_MAGIC_OUTPUT"] = "False"

        if not result:
            return

        self.shell.user_ns["_taegis_magic_result"] = result

        if magic_args:
            if magic_args.assign:
                if isinstance(result.results, list):
                    self.shell.user_ns[magic_args.assign] = pd.json_normalize(
                        result.results, max_level=3
                    ).dropna(how="all", axis=1)
                else:
                    self.shell.user_ns[magic_args.assign] = result.results

            elif magic_args.append:
                if isinstance(result.results, list):
                    if magic_args.append not in self.shell.user_ns:
                        self.shell.user_ns[magic_args.append] = pd.DataFrame()

                    self.shell.user_ns[magic_args.append] = pd.concat(
                        [
                            self.shell.user_ns[magic_args.append],
                            pd.json_normalize(result.results, max_level=3).dropna(
                                how="all", axis=1
                            ),
                        ]
                    ).reset_index(drop=True)

                else:
                    if magic_args.append not in self.shell.user_ns:
                        self.shell.user_ns[magic_args.append] = []

                    self.shell.user_ns[magic_args.append].append(result.results)

            if magic_args.display:
                display_markdown(
                    self.shell.user_ns[magic_args.display].to_html(), raw=True
                )
                return

        if magic_args.cache:
            display_cache(magic_args.assign, cache_digest, result)
            display(Javascript("IPython.notebook.save_checkpoint();"), exclude=["text/plain"])
        else:
            display(result, exclude=["text/plain"])


def load_ipython_extension(ipython):
    """
    Load Taegis Magics extension.

    Parameters
    ----------
    ipython : InteractiveShell
        IPython session
    """
    ipython.register_magics(TaegisMagics)
