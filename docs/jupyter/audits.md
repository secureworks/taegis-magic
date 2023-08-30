# Taegis Magic

## Audits

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [Audits](notebooks/Audits.ipynb)

### Audit Differences

Taegis audit logs contain before and after states.  Taegis Magic can parse the states to just provide the difference in the changed fields with the pipe function `get_diffs`.

```python
from taegis_magic.pandas.audits import get_diffs
```

```python
%taegis audits search --application investigations --action update --assign investigation_audits
```

```python
investigation_update_diffs = investigation_audits.pipe(get_diffs)

investigation_update_diffs[[
    column
    for column in investigation_update_diffs.columns
    if column.startswith("taegis_magic.diff.")
]]
```
