import importlib.resources as pkg_resources
import inspect
import logging
import sys
import traceback
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List, Optional

import jupyter_client
import nbclient
import papermill
import typer
from typing_extensions import Annotated

from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResult
from taegis_magic.core.notebook import generate_report
from taegis_magic.core.service import get_service

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Notebook Commands.")


@dataclass
class NotebookResult:
    action: str


class LOG_LEVEL(str, Enum):
    NOTSET = "NOTSET"
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@app.command()
@tracing
def execute(
    input_notebook: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=True,
            readable=True,
            resolve_path=True,
            help="Input notebook to parameterize and execute.",
        ),
    ],
    output_notebook: Annotated[
        Optional[Path],
        typer.Argument(
            exists=False,
            file_okay=True,
            dir_okay=False,
            writable=True,
            readable=True,
            resolve_path=True,
            help="Output notebook with parameters and results.",
        ),
    ] = None,
    parameter: Annotated[
        Optional[List[str]],
        typer.Option(
            "--parameter",
            "-p",
            help="Parameters to pass to the parameters cell. Use the format PARAMETER_NAME=VALUE.",
        ),
    ] = None,
    inject_input_path: Annotated[
        bool,
        typer.Option(
            "--inject-input-path",
            help="Insert the path of the input notebook as PAPERMILL_INPUT_PATH as a notebook parameter.",
        ),
    ] = False,
    inject_output_path: Annotated[
        bool,
        typer.Option(
            "--inject-output-path",
            help="Insert the path of the output notebook as PAPERMILL_OUTPUT_PATH as a notebook parameter.",
        ),
    ] = False,
    inject_paths: Annotated[
        bool,
        typer.Option(
            "--inject-paths",
            help="Insert the paths of input/output notebooks as PAPERMILL_INPUT_PATH/PAPERMILL_OUTPUT_PATH"
            " as notebook parameters.",
        ),
    ] = False,
    engine: Annotated[
        Optional[str],
        typer.Option(
            "--engine",
            help="The execution engine name to use in evaluating the notebook.",
        ),
    ] = None,
    request_save_on_cell_execute: Annotated[
        bool,
        typer.Option(
            "--request-save-on-cell-execute",
            help="Request save notebook after each cell execution",
        ),
    ] = True,
    autosave_cell_every: Annotated[
        int,
        typer.Option(
            "--autosave-cell-every",
            help="How often in seconds to autosave the notebook during long cell executions (0 to disable)",
        ),
    ] = 30,
    prepare_only: Annotated[
        bool,
        typer.Option(
            "--prepare-only/--prepare-execute",
            help="Flag for outputting the notebook without execution, but with parameters applied.",
        ),
    ] = False,
    kernel: Annotated[
        Optional[str],
        typer.Option(
            "--kernel",
            "-k",
            help="Name of kernel to run. Ignores kernel name in the notebook document metadata.",
        ),
    ] = None,
    language: Annotated[
        Optional[str],
        typer.Option(
            "--language",
            "-l",
            help="Language for notebook execution. Ignores language in the notebook document metadata.",
        ),
    ] = None,
    cwd: Annotated[
        Optional[str],
        typer.Option(
            "--cwd",
            help="Working directory to run notebook in.",
        ),
    ] = None,
    progress_bar: Annotated[
        bool,
        typer.Option(
            "--progress-bar/--no-progress-bar",
            help="Flag for turning on the progress bar.",
        ),
    ] = True,
    log_output: Annotated[
        bool,
        typer.Option(
            "--log-output/--no-log-output",
            help="Flag for writing notebook output to the configured logger.",
        ),
    ] = False,
    log_level: Annotated[
        LOG_LEVEL, typer.Option("--log-level", help="Log level to use.")
    ] = LOG_LEVEL.INFO,
    stdout_file: Annotated[
        Optional[typer.FileTextWrite],
        typer.Option(
            mode="w",
            encoding="utf-8",
            help="File to write notebook stdout output to.",
        ),
    ] = None,
    stderr_file: Annotated[
        Optional[typer.FileTextWrite],
        typer.Option(
            mode="w",
            encoding="utf-8",
            help="File to write notebook stderr output to.",
        ),
    ] = None,
    start_timeout: Annotated[
        int,
        typer.Option(
            "--start-timeout",
            help="Time in seconds to wait for the kernel to start.",
        ),
    ] = 60,
    execution_timeout: Annotated[
        int,
        typer.Option(
            "--execution-timeout",
            help="Time in seconds to wait for each cell before failing execution (default: forever)",
        ),
    ] = None,
    report_mode: Annotated[
        bool,
        typer.Option("--report-mode/--no-report-mode", help="Flag for hiding input."),
    ] = False,
    title: Annotated[Optional[str], typer.Option(help="Investigation Title.")] = None,
    tenant: Annotated[Optional[str], typer.Option(help="Taegis Tenant ID.")] = None,
    region: Annotated[Optional[str], typer.Option(help="Taegis Region.")] = None,
):
    """Execute a Jupyter notebook."""
    if not prepare_only:
        service = get_service(tenant_id=tenant, environment=region)
        service.access_token

    input_notebook = input_notebook or "-"

    if output_notebook is None:
        stem = input_notebook.stem
        if region:
            stem = f"{stem}_{region}"
        if tenant:
            stem = f"{stem}_{tenant}"

        stem = f"{stem}{input_notebook.suffix}"

        if stem != input_notebook.stem:
            output_notebook = input_notebook.with_name(stem)

    output_notebook = output_notebook or "-"

    if output_notebook == "-":
        # Save notebook to stdout just once
        request_save_on_cell_execute = False

        # Reduce default log level if we pipe to stdout
        if log_level == LOG_LEVEL.INFO:
            log_level = LOG_LEVEL.ERROR

    logging.basicConfig(level=log_level.value, format="%(message)s")

    parameters = {}
    bad_parameters = []
    if inject_input_path or inject_paths:
        parameters["PAPERMILL_INPUT_PATH"] = str(input_notebook)
    if inject_output_path or inject_paths:
        parameters["PAPERMILL_OUTPUT_PATH"] = str(output_notebook)
    for param in parameter or []:
        try:
            name, value = param.split("=", maxsplit=1)
        except ValueError:
            bad_parameters.append(param)
            continue

        parameters[name] = value

    if title:
        parameters["INVESTIGATION_TITLE"] = title
    if tenant:
        parameters["TENANT_ID"] = tenant
    if region:
        parameters["REGION"] = region

    if bad_parameters:
        print(
            f"Please ensure the following parameters use the PARAMETER_NAME=VALUE format: {bad_parameters}"
        )
        raise typer.Exit()

    if "TAEGIS_MAGIC_NOTEBOOK_FILENAME" not in parameters:
        if output_notebook and output_notebook != "-":
            parameters["TAEGIS_MAGIC_NOTEBOOK_FILENAME"] = str(
                output_notebook.resolve()
            )
        else:
            parameters["TAEGIS_MAGIC_NOTEBOOK_FILENAME"] = str(input_notebook.resolve())

    try:
        papermill.execute_notebook(
            input_path=input_notebook,
            output_path=output_notebook,
            parameters=parameters,
            engine_name=engine,
            request_save_on_cell_execute=request_save_on_cell_execute,
            autosave_cell_every=autosave_cell_every,
            prepare_only=prepare_only,
            kernel_name=kernel,
            language=language,
            progress_bar=progress_bar,
            log_output=log_output,
            stdout_file=stdout_file,
            stderr_file=stderr_file,
            start_timeout=start_timeout,
            report_mode=report_mode,
            cwd=cwd,
            execution_timeout=execution_timeout,
        )
    except nbclient.exceptions.DeadKernelError:
        # Exiting with a special exit code for dead kernels
        traceback.print_exc()
        sys.exit(138)

    """Execute the current notebook."""
    return TaegisResult(
        raw_results=NotebookResult(action="execute"),
        service="notebook",
        tenant_id=None,
        region=None,
        arguments=inspect.currentframe().f_locals,
    )


@app.command(name="generate-report")
@tracing
def generate(input_notebook: Annotated[str, typer.Argument(...)]):
    """Generate a report from the current notebook."""
    report = generate_report(filename=input_notebook)

    return TaegisResult(
        raw_results=NotebookResult(action="generate_report"),
        service="notebook",
        tenant_id=None,
        region=None,
        arguments=inspect.currentframe().f_locals,
    )


@app.command()
@tracing
def create(
    output_notebook: Annotated[
        Path,
        typer.Argument(
            exists=False,
            file_okay=True,
            dir_okay=False,
            writable=True,
            readable=True,
            resolve_path=True,
        ),
    ],
):
    """Create a Jupyter Notebook to run against Taegis."""
    template = pkg_resources.read_text("taegis_magic.templates", "template.ipynb")

    output_notebook.write_text(template)

    return TaegisResult(
        raw_results=NotebookResult(action="create"),
        service="notebook",
        tenant_id=None,
        region=None,
        arguments=inspect.currentframe().f_locals,
    )
