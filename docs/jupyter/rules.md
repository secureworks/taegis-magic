# Taegis Magic

## Rules

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [Rules](notebooks/Rules.ipynb)


### Filters

Taegis rules (mostly suppression) can have multiple filters.  Users can set the filter criteria side by side with the rule data with pipe function `inflate_filters`.

Rules may have been created from multiple filters.  This function will expand the DataFrame so each filter will be along side the rule data (potentially duplicating rule data) and set the `filters` columns side by side with the alert data in columns preprended with `filters.`.

```python
from taegis_magic.pandas.rules import inflate_filters
```

```python
%taegis rules suppression --assign suppression_rules
```

```python
inflated_suppression_rules = suppression_rules.pipe(inflate_filters)

set(inflated_suppression_rules.columns).difference(set(suppression_rules.columns))
```
