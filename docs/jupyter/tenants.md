# Taegis Magic

## Tenants

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [Tenants](notebooks/Tenants.ipynb)

```
%taegis tenants search --assign tenants
```

### Inflate Environments

Taegis tenants may be in different environments (or regions).  This function will inflate the different environments to be set side by side to the tenant data.  New columns will consist of the environment name, the `{name}.created_at` and `{name}.update_at`.

```python
from taegis_magic.pandas.tenants import inflate_environments
```

```python
inflated_environments_df = tenants.pipe(inflate_environments)
```
