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

### Lookup Tenants

Data can be correlated to Tenant data with `lookup_tenants`.  This pipe function takes a `region` parameter and an optional `tenant_id_column`.  `tenant_id_column` will set to `tenant_id` or `tenant.id` if not provided.  This pipe function will work on any dataset that contains a tenant reference.  Any original data that cannot be correlated to a tenant will have the new columns contain Numpy NaN values.

Correlated tenant columns will be prepended with `tenant.`.  If the original dataset has shared column names, the new tenant column will be suffixed with `.lookup_tenants`.

```python
from taegis_magic.pandas.tenants import lookup_tenants
```

Example:

```python
correlated_alerts = alerts.pipe(lookup_tenants, region="US1")
```
