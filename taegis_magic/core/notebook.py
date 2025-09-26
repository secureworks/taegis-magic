import logging
import os
import re
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from importlib.metadata import version as module_version
from pathlib import Path
from time import sleep
from typing import List, Optional, Union

import jinja2
import nbconvert
import papermill
from ipylab import JupyterFrontEnd
from IPython import get_ipython
from IPython.display import HTML, Javascript, display
from nbconvert.preprocessors.tagremove import TagRemovePreprocessor
from packaging.version import Version
from packaging.version import parse as parse_version
from traitlets.config import Config

log = logging.getLogger(__name__)

VERSION_6 = parse_version("6")
VERSION_7 = parse_version("7")
VERSION_8 = parse_version("8")


def pool_initializer():
    """Default ProcessPoolExecutor initializer.

    https://github.com/ipython/ipython/issues/11049
    """

    sys.stdout.write(" ")
    sys.stdout.flush()


@dataclass
class NotebookContext:
    """Metadata for a Jupyter notebook."""

    tenant: str
    region: str
    parameters: Optional[dict] = None


@dataclass
class NotebookScope:
    """Scope for a Jupyter notebook execution."""

    tenant: str
    region: str
    notebook_title: Optional[str] = None
    notebook_path: Optional[Union[str, Path]] = None


def get_notebook_version() -> Version:
    return parse_version(module_version("notebook"))


def find_notebook_name() -> Optional[str]:
    """Find the name of the current notebook."""
    notebook_name = None

    if not notebook_name:
        ip = get_ipython()
        if notebook_name := ip.user_ns.get("__vsc_ipynb_file__"):
            notebook_name = Path(notebook_name).name
            log.debug(f"Notebook found via vscode: {notebook_name}")
        else:
            log.debug("Could not find notebook name using __vsc_ipynb_file__")

    if not notebook_name:
        if notebook_name := os.environ.get("JPY_SESSION_NAME"):
            notebook_name = Path(notebook_name).name
            log.debug(f"Notebook found via JPY_SESSION_NAME: {notebook_name}")
        else:
            log.debug("Could not find notebook name using JPY_SESSION_NAME")

    if not notebook_name:
        try:
            # not best practice, but there are some load time issues
            # with ipynbname that we need to work through
            # if we don't need to load it, we shouldn't
            import ipynbname

            notebook_name = ipynbname.path().name
            log.debug(f"Notebook found via ipynbname: {notebook_name}")
        except Exception as e:
            log.debug(f"Error finding notebook name using ipynbname: {e}")

    return notebook_name


def save_notebook(delay: int = 0):
    """Save the current notebook."""
    ip = get_ipython()
    if "__vsc_ipynb_file__" in ip.user_ns:
        log.error(
            "save_notebook does not work in VS Code notebooks, please save manually before proceeding."
        )
        return

    notebook_version = get_notebook_version()
    log.debug(f"Notebook version: {notebook_version}")

    if notebook_version >= VERSION_6 and notebook_version < VERSION_7:
        display('<-- #region tags=["remove_cell"] -->')

        try:
            script = """
            this.nextElementSibling.focus();
            this.dispatchEvent(new KeyboardEvent('keydown', {key:'s', keyCode: 83, ctrlKey: true}));
            """
            display(
                HTML(
                    (
                        '<img src onerror="{}" style="display:none">'
                        '<input style="width:0;height:0;border:0">'
                    ).format(script)
                )
            )
        except Exception:
            pass

        try:
            display(Javascript("IPython.notebook.save_checkpoint();"))
        except Exception:
            pass

        display("<-- #endregion -->")

    elif notebook_version >= VERSION_7 and notebook_version < VERSION_8:
        try:
            app = JupyterFrontEnd()
            app.commands.execute("docmanager:save")
        except Exception:
            pass

    else:
        log.error(
            "Cannot save notebook, unsupported notebook version.  Please save manually before proceeding."
        )
        return

    sleep(delay)


def remove_region_tags(body: str) -> str:
    """Remove region tags from a report that may not be related to cells."""
    region_pattern = (
        r'\'<--\s*#region\s*tags=\["remove_cell"\]\s*-->(.*?)<--\s*#endregion\s*-->\''
    )
    body = re.sub(region_pattern, "", body, flags=re.DOTALL)
    return body


def generate_report(filename: Union[str, Path]) -> Path:
    """Takes path to Jupyter notebook and uses nbconvert
    to export notebook as markdown.

    Uses nbconvert preprocessors to allow users to
    manually tag notebook cells for inclusion/exclusion
    in the output.

    Also uses jinja templates to override notebook
    output. By default, all code cells, cell input,
    raw cells, and streaming output is excluded.
    These templates live in `common.templates`.

    Parameters
    ----------
    filename : str | Path
        Notebook file name

    Raises
    ------
    FileNotFoundError
        If the file does not exist
    """

    # Configure our tag removal. I believe we can add/change
    # the cell tags that will trigger include/exclude behavior
    # in the tuples below.
    # save_notebook()

    log.debug("Configuring nbconvert preprocessor...")

    if isinstance(filename, str):
        filename = Path(filename)

    if not filename.exists():
        raise FileNotFoundError(f"File {filename} does not exist")

    c = Config()
    c["TagRemovePreprocessor"].remove_cell_tags = ("remove_cell",)
    c["TagRemovePreprocessor"].remove_all_outputs_tags = ("remove_output",)
    c["TagRemovePreprocessor"].remove_input_tags = ("remove_input",)

    c.MarkdownExporter.preprocessors = [TagRemovePreprocessor]
    exporter = nbconvert.MarkdownExporter(config=c)
    template_file = "jupyter_extended_markdown_template.jinja"

    # The nbconvert exporter has its own jinja environment.
    # We need to point that environment to our templates
    # directory to allow them to be loaded. Otherwise defaults
    # to the local directory and off Jupyter path
    exporter.environment.loader.loaders.append(
        jinja2.PackageLoader("taegis_magic", "templates")
    )

    # log.debug(exporter.environment.loader.list_templates())

    # Not sure how this interops with `report_mode` in `papermill`
    exporter.exclude_input_prompt = True
    exporter.exclude_output_prompt = True
    exporter.exclude_input = True
    exporter.exclude_raw = True
    exporter.template_file = template_file

    output_file = filename.with_suffix(".report.md")
    body, _ = exporter.from_filename(filename)

    # clear output from --cache
    body = remove_region_tags(body)

    log.info(f"Writing markdown output to {str(output_file.resolve())}")
    Path(output_file).write_text(body, encoding="utf-8", errors="replace")

    return output_file


def execute_notebook_pool(
    notebook_context: List[NotebookContext],
    notebook_path: Union[str, Path],
    notebook_title: str,
    process_pool_kwargs: Optional[dict] = None,
    papermill_kwargs: Optional[dict] = None,
):
    """Execute a notebook in parallel using papermill and ProcessPoolExecutor.

    Parameters
    ----------
    notebook_context : List[NotebookContext]
        List of notebook contexts to execute.
    notebook_path : Union[str, Path]
        File path to notebook to execute.
    notebook_title : str
        Case Title for the notebook.
    process_pool_kwargs : Optional[dict], optional
        ProcessPoolExecutor keyword arguments, by default None
    papermill_kwargs : Optional[dict], optional
        Papermill keyword arguments, by default None

    Raises
    ------
    FileNotFoundError
    """
    if process_pool_kwargs is None:
        process_pool_kwargs = {}
    if papermill_kwargs is None:
        papermill_kwargs = {}

    if process_pool_kwargs.get("initializer") is None:
        process_pool_kwargs["initializer"] = pool_initializer

    if isinstance(notebook_path, str):
        notebook_path = Path(notebook_path)
    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook {notebook_path} does not exist")

    executed_procedures = {}
    with ProcessPoolExecutor(**process_pool_kwargs) as executor:
        for context in notebook_context:
            if papermill_kwargs.get("output_notebook") is None:
                stem = notebook_path.stem
                if context.region:
                    stem = f"{stem}_{context.region}"
                if context.tenant:
                    stem = f"{stem}_{context.tenant}"

                stem = f"{stem}{notebook_path.suffix}"

                if stem != notebook_path.stem:
                    output_notebook = notebook_path.with_name(stem)

            parameters = context.parameters or {}

            if papermill_kwargs.get("inject_input_path") or papermill_kwargs.get(
                "inject_paths"
            ):
                parameters["PAPERMILL_INPUT_PATH"] = str(notebook_path)
            if papermill_kwargs.get("inject_output_path") or papermill_kwargs.get(
                "inject_paths"
            ):
                parameters["PAPERMILL_OUTPUT_PATH"] = str(output_notebook)

            if notebook_title:
                parameters["INVESTIGATION_TITLE"] = notebook_title
            if context.tenant:
                parameters["TENANT_ID"] = context.tenant
            if context.region:
                parameters["REGION"] = context.region

            if "TAEGIS_MAGIC_NOTEBOOK_FILENAME" not in parameters:
                if output_notebook and output_notebook != "-":
                    parameters["TAEGIS_MAGIC_NOTEBOOK_FILENAME"] = str(
                        output_notebook.resolve()
                    )
                else:
                    parameters["TAEGIS_MAGIC_NOTEBOOK_FILENAME"] = str(
                        notebook_path.resolve()
                    )

            executed_procedures[
                executor.submit(
                    papermill.execute_notebook,
                    input_path=notebook_path,
                    output_path=output_notebook,
                    parameters=parameters,
                    **papermill_kwargs,
                )
            ] = NotebookScope(
                tenant=context.tenant,
                region=context.region,
                notebook_title=notebook_title,
                notebook_path=notebook_path,
            )

    # Wait for the notebooks to finish running
    # and log exceptions as they occur.
    for i, executed_procedure in enumerate(as_completed(executed_procedures)):
        procedure_scope = executed_procedures[executed_procedure]
        log.debug(
            f"{i+1}/{len(executed_procedures.keys())} {procedure_scope} Finished!"
        )
        exc = executed_procedure.exception()
        if exc:
            log.error(procedure_scope, exc)
