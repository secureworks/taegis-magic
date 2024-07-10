import logging
from pathlib import Path

import jinja2
import nbconvert
from nbconvert.preprocessors.tagremove import TagRemovePreprocessor
from traitlets.config import Config


from typing import Union

log = logging.getLogger(__name__)

from IPython.display import display, HTML
from time import sleep


def save_notebook(delay: int = 0):
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
    sleep(delay)


def save_report(filename: Union[str, Path]) -> Path:
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

    log.debug(exporter.environment.loader.list_templates())

    # Not sure how this interops with `report_mode` in `papermill`
    exporter.exclude_input_prompt = True
    exporter.exclude_output_prompt = True
    exporter.exclude_input = True
    exporter.exclude_raw = True
    exporter.template_file = template_file

    output_file = filename.with_suffix(".report.md")
    body, _ = exporter.from_filename(filename)
    log.info(f"Writing markdown output to {str(output_file.resolve())}")
    Path(output_file).write_text(body, encoding="utf-8", errors="replace")

    return output_file
