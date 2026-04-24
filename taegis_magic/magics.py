"""Taegis IPython Magics."""

import hashlib
import logging
import shlex
from argparse import ArgumentError, ArgumentParser
from pathlib import Path
from textwrap import dedent
from typing import Optional

import pandas as pd
from gql.transport.exceptions import TransportQueryError
from IPython.core.magic import Magics, line_cell_magic, line_magic, magics_class
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython.display import display, display_markdown
from jinja2 import TemplateSyntaxError
from taegis_magic.cli import app
from taegis_magic.commands.configure import set_defaults
from taegis_magic.core.log import (
    TRACE_LOG_LEVEL,
    get_module_logger,
    get_sdk_logger,
)
from taegis_magic.core.cache import (
    decode_base64_obj_as_pickle,
    display_cache,
    get_cache_item,
)
from taegis_magic.core.notebook import (
    find_notebook_name,
    generate_report,
    save_notebook,
)

from taegis_sdk_python.templates import load_jinja2_template_environment
from taegis_sdk_python.config import get_config
from taegis_magic.commands.configure import QUERIES_SECTION, DisableReturnDisplay

TAEGIS_MAGIC_NOTEBOOK_FILENAME = "TAEGIS_MAGIC_NOTEBOOK_FILENAME"

log = logging.getLogger(__name__)
set_defaults()
taegis_magic_logger = get_module_logger()
sdk_logger = get_sdk_logger()


def _set_temporary_log_levels(command_args: list[str]):
    """Temporarily override logger levels when CLI flags are passed."""
    original_magic_log_level = taegis_magic_logger.getEffectiveLevel()
    original_sdk_log_level = sdk_logger.getEffectiveLevel()

    trace = "--trace" in command_args
    debug = "--debug" in command_args
    verbose = "--verbose" in command_args
    warning = "--warning" in command_args
    error = "--error" in command_args

    sdk_debug = "--sdk-debug" in command_args
    sdk_verbose = "--sdk-verbose" in command_args
    sdk_warning = "--sdk-warning" in command_args
    sdk_error = "--sdk-error" in command_args

    magic_option_provided = any([trace, debug, verbose, warning, error])
    sdk_option_provided = any([sdk_debug, sdk_verbose, sdk_warning, sdk_error])

    if trace:
        taegis_magic_logger.setLevel(TRACE_LOG_LEVEL)
    elif debug:
        taegis_magic_logger.setLevel(logging.DEBUG)
    elif verbose:
        taegis_magic_logger.setLevel(logging.INFO)
    elif warning:
        taegis_magic_logger.setLevel(logging.WARNING)
    elif error:
        taegis_magic_logger.setLevel(logging.ERROR)

    if sdk_debug:
        sdk_logger.setLevel(logging.DEBUG)
    elif sdk_verbose:
        sdk_logger.setLevel(logging.INFO)
    elif sdk_warning:
        sdk_logger.setLevel(logging.WARNING)
    elif sdk_error:
        sdk_logger.setLevel(logging.ERROR)

    def restore_original_log_levels():
        if magic_option_provided:
            taegis_magic_logger.setLevel(original_magic_log_level)
        if sdk_option_provided:
            sdk_logger.setLevel(original_sdk_log_level)

    return restore_original_log_levels


def taegis_magics_command_parser() -> ArgumentParser:
    """Magics support flags."""
    config = get_config()

    parser = ArgumentParser("taegis_magic_parser", allow_abbrev=False)
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
        "--disable-return-display",
        choices=[e.value for e in DisableReturnDisplay],
        default=config[QUERIES_SECTION].get(
            "disable-return-display", DisableReturnDisplay.off.value
        ),
        help="Disable automatic display of return metadata table, does not work with --cache",
    )
    parser.add_argument("--cell", help="Cell contents")
    parser.add_argument(
        "-t",
        "--cell-template",
        action="store_true",
        help="Use a Jinja2 Template for input, uses Shell Namespace as variables",
    )
    parser.add_argument(
        "-f",
        "--cell-template-file",
        type=str,
        help="Use a template file instead of providing cell input",
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

        log.debug("Trying to get notebook name for IPython shell...")
        # try to get the notebook name from the environment
        notebook_name = (
            self.shell.user_ns.get(TAEGIS_MAGIC_NOTEBOOK_FILENAME)
            or self.shell.user_ns.get("PAPERMILL_OUTPUT_PATH")
            or self.shell.user_ns.get("PAPERMILL_INPUT_PATH")
        )

        # try to find it through other means
        if not notebook_name:
            log.debug("Notebook name not found in environment variables...")
            try:
                notebook_name = find_notebook_name()
            except Exception as e:
                log.error(f"Error finding notebook name: {e}")
                notebook_name = None

        if notebook_name:
            log.debug(f"Notebook name found: {notebook_name}")
            self.shell.user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME] = notebook_name
            if not self.shell.user_ns.get("REPORT_TITLE"):
                self.shell.user_ns["REPORT_TITLE"] = Path(notebook_name).stem.title()
        else:
            log.error(
                f"Could not determine notebook name. Please set {TAEGIS_MAGIC_NOTEBOOK_FILENAME} manually."
            )

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
                log.warning("Argument parsing failed, exiting magic...")
                return
        except ArgumentError as e:
            log.error(f"Invalid argument: {e}")
            return

        restore_log_levels = _set_temporary_log_levels(command_args or [])
        try:
            log.debug(f"Magic Args: {magic_args}")
            log.debug(f"Command Args: {command_args}")

            if magic_args and magic_args.cell:
                cell = magic_args.cell

            if magic_args and magic_args.cell_template:
                template_environment = load_jinja2_template_environment()
                try:
                    if magic_args.cell_template_file:
                        log.debug(f"Loading template file: {magic_args.cell_template_file}")
                        template = template_environment.get_template(
                            magic_args.cell_template_file
                        )
                    elif cell:
                        template = template_environment.from_string(cell)
                    else:
                        raise ValueError("Cell contents or template file not provided")
                except TemplateSyntaxError as exc:
                    log.error(f"Invalid template: {exc}")
                    return

                cell = template.render(**self.shell.user_ns)
                log.debug("Template rendered successfully")

            if magic_args and magic_args.cache:
                if not magic_args.assign:
                    raise ValueError("--assign must be set with cache...")

                if (
                    TAEGIS_MAGIC_NOTEBOOK_FILENAME in self.shell.user_ns
                    and self.shell.user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME]
                ):
                    notebook_filename = self.shell.user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME]
                else:
                    log.warning(f"{TAEGIS_MAGIC_NOTEBOOK_FILENAME} not set, prompting for input...")
                    notebook_filename = input("Notebook Filename:")

                if not notebook_filename:
                    raise ValueError("Cannot determine file name of notebook...")

                notebook_fp = Path(notebook_filename)

                if notebook_fp.exists():
                    self.shell.user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME] = notebook_filename
                else:
                    raise ValueError(
                        f"Notebook {notebook_filename} does not exist on disk, save notebook to disk before caching or "
                        "reload the taegis_magic extension (%reload_ext taegis_magic)..."
                    )

                if not cell:
                    cell = ""

                cache_digest = hashlib.sha256(bytes(line + cell, "utf-8")).hexdigest()
                cache = get_cache_item(notebook_fp, magic_args.assign, cache_digest)
                if cache:
                    log.info(f"{magic_args.assign} found in cache...")
                    log.debug("Normalizing results...")
                    data = decode_base64_obj_as_pickle(cache.get("data"))

                    log.debug("Converting to dataframe...")
                    self.shell.user_ns[magic_args.assign] = pd.json_normalize(
                        data.results, max_level=3
                    ).dropna(axis=1, how="all")

                    log.info(f"Resetting {magic_args.assign}:{cache_digest} to cache...")
                    display_cache(magic_args.assign, cache_digest, data)
                    save_notebook()

                    return
                else:
                    log.info(f"{magic_args.assign} not found in cache...")

            if cell:
                command_args.extend(["--cell", cell])

            # os.environ["TAEGIS_MAGIC_OUTPUT"] = "True"
            try:
                result = app(command_args, prog_name="taegis", standalone_mode=False)
            except (SystemExit, TransportQueryError) as e:
                log.exception(f"Command execution failed: {type(e).__name__}: {e}")
                result = None
            # os.environ["TAEGIS_MAGIC_OUTPUT"] = "False"

            if not result:
                return

            self.shell.user_ns["_taegis_magic_result"] = result
            self.shell.user_ns["_taegis_magic_cell_contents"] = cell

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
                save_notebook()
            else:
                if magic_args.disable_return_display == "all":
                    pass
                elif (
                    magic_args.disable_return_display == "on_empty"
                    and result.results_returned == 0
                ):
                    pass
                else:
                    display(result, exclude=["text/plain"])
        finally:
            restore_log_levels()

    @magic_arguments()
    @argument(
        "--delay",
        type=int,
        default=0,
        help=("Delay in seconds while saving the notebook, defaults to 0 seconds"),
    )
    @line_magic
    def save_notebook(self, line: str):
        """Save the current notebook."""
        args = parse_argstring(self.save_notebook, line)
        save_notebook(delay=args.delay)

    @line_magic
    def generate_report(self, line: str = ""):
        """Save the current notebook as a report.

        Sets the TAEGIS_MAGIC_REPORT_FILENAME variable in the user namespace."""
        if (
            TAEGIS_MAGIC_NOTEBOOK_FILENAME not in self.shell.user_ns
            or not self.shell.user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME]
        ):
            raise ValueError(
                f"Cannot determine file name of notebook. Please set {TAEGIS_MAGIC_NOTEBOOK_FILENAME}."
            )

        if not Path(self.shell.user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME]).exists():
            raise FileNotFoundError(
                dedent(
                    f"""Notebook {self.shell.user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME]} does not exist.
                
                    Save notebook manually or run %save_notebook to save the notebook to disk.
                    """
                )
            )

        report = generate_report(
            filename=self.shell.user_ns[TAEGIS_MAGIC_NOTEBOOK_FILENAME]
        )
        self.shell.user_ns["TAEGIS_MAGIC_REPORT_FILENAME"] = str(report.resolve())


def load_ipython_extension(ipython):
    """
    Load Taegis Magics extension.

    Parameters
    ----------
    ipython : InteractiveShell
        IPython session
    """
    ipython.register_magics(TaegisMagics)
