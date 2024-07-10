# Taegis Magic

The Taegis Magic provide two helper magics to automatically generate a report to include in Investigation keyfindings.

 * `%save_notebook [--delay seconds]`
 * `%save_report`

## Saving a notebook

Use the `%save_notebook` magic.

Options:
 * `--delay [seconds]` ; used to delay the cell progressing to the next.  Useful when attempting to save a large notebook to disk and need some time to allow the write buffer to flush.

## Saving a markdown report

Use the `%save_report` magic.

Notes: 

 * Needs the `TAEGIS_MAGIC_NOTEBOOK_FILENAME` variable set in the namespace.  This is provided within `%load_ext taegis_magic`.
 * Sets the `TAEGIS_MAGIC_REPORT_FILENAME` variable to the namespace.  This is useful for referencing in `%taegis investigations create --keyfindings $TAEGIS_MAGIC_REPORT_FILENAME ...` command.

Cell tags may be used to help to help craft the output from the notebook into the report.  This may be useful for internal analysis cells or markdown notes to present to an analyst that you would not want to be presented in the final report.

Tags:
 * `remove_cell`: this removes all reference to the cell in the report
 * `remove_input`: this removes the input references from a cell in the report
 * `remove_output`: this removes the cell output from the report

Cell tags in Jupyter Client 8+ can be managed in `View->Right Sidebar->Show Notebook Tools` and expanding the `Common Tools` section.

If using jupytext to manage notebooks in a markdown format.  Cell tags can be referenced as below:

For Python cells:

````
```python tags=["remove_cell"]
code
```
````

For Markdown cells:

````
<!-- #region tags=["remove_cell"] -->
### Header
 Internal Notes
<!-- #endregion -->
````
