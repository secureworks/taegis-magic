# Taegis Magic

## Jupyter

### Getting Started

```python
%load_ext taegis_magic
```

Find an example notebook with these examples in [Getting Started](notebooks/Getting_Started.ipynb)


### IPython Magics

[IPython Magics](https://ipython.readthedocs.io/en/stable/interactive/magics.html) are a form of helper functionality within an IPython kernel.  The Taegis Magic are designed to use the IPython Magics interface to provide command-line level notebook functionality so that Taegis users can use notebooks without needing a deep understanding of Python or programming.

`%` signify a line magic.  They read in a single line of text for parsing.

Example:

```
%taegis users current-user --assign me --display me
```

`%%` signify a cell magic.  They read in the top line as a line magic.  The remaining cell lines are read as the cell magic portion.

Example:

```
%%taegis alerts search --assign alerts
FROM alert
WHERE
    severity >= 0.6 AND
    status = 'OPEN' AND
    investigation_ids IS NULL
EARLIEST=-1d | head 5
```

### IPython Magics Specific Options

Run the `--help` menu from within a Jupyter notebook cell to access an extra help menu.

```
%taegis --help
```

```
usage: taegis_magic_parser [-h] [--assign NAME | --append NAME]
                           [--display NAME] [--cache]

optional arguments:
  -h, --help      show this help message and exit
  --assign NAME   Assign results as pandas DataFrame to NAME
  --append NAME   Append results as pandas DataFrame to NAME
  --display NAME  Display NAME as markdown table
  --cache         Save output to cache / Load output from cache (if present)
```

These extras are to help users with Taegis command results.

`--assign` creates or overwrites a variable in the Jupyter notebook with `[NAME]` and assigns it as a Pandas DataFrame.

`--append` creates or appends a variable in the Jupyter notebook with `[NAME]` and assigns it as a Pandas DataFrame.

`--display` displays `[NAME]` as a markdown table.  This is useful for results with small result sets.

`--cache` stores the results in the cell output for later reference.  This is useful for reloading data to its original state without needing to re-query the API, automating the data gathering portion for a notebook for later review by a user, or saving data in the notebook to send to another user.  The cache is content aware, so changing the contents will force the magic to call the API again.  The cache may be reset by clearing the output on the cell.

### String Interpolation

You can chain commands together using string interpolation within the line magic portion of the Taegis Magic.  The following will not work in the cell portion.

For this example, let's assume that you have a list of unfamiliar terms and wants to search Taegis Threat Publications for more information.

```python
terms = [
    "mimikatz",
    "Cobalt Strike",
    "GOLD FIESTA",
]
for term in terms:
    %taegis threat publications search "$term" --append pubs
```

This conducts an API call to query for threat publications on each term and appends them into the same DataFrame called `pubs` for review.  Place `$term` in quotes `""` or `''` in case the variable contains whitespace.  The `$term` is replaced by the string value in the variable.

Since this does not work in the cell portion when calling the magic, we can still get around this limitation by using the `--cell` option instead.

For this example, let's assume you query `alerts`, parse the defined fields and wants to pivot to events.

```python
user = "admin"
query = f"FROM process WHERE @user CONTAINS '{user}' EARLIEST=-3d"
%taegis events search --cell "$query" --assign events
```

This showcases three parts: the parsed field, the generic query, passing the query via `--cell`.  Normally this would be done in multiple cells.

## Further Reading

* [Alerts](alerts.md)
* [Audits](audits.md)
* [Investigations](investigations.md) 
* [Rules](rules.md)
* [Threat](threat.md)
* [Save Notebook and Save Report](save_report.md)
