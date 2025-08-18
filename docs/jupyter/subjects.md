# Taegis Magic

## Subjects

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [Subjects](notebooks/Subjects.ipynb)

### Lookup Users

Look up users and correlate to the input dataframe.

```
Parameters
----------
df : pd.DataFrame
    Dataframe
region : str
    Taegis Region
tenant_id : Optional[str], optional
    Tenant ID to use for the lookup, by default None.
user_id_columns : Optional[List[str]], optional
    List of user id columns to look up, by default None.
    If None, defaults to ['created_by', 'updated_by'].
merge_ons : Optional[List[str]], optional
    List of user id fields to merge on, by default None.
    If None, defaults to ['user.id', 'user.idp_user_id', 'user.user_id']

Returns
-------
pd.DataFrame
    Dataframe with correlated User.
    New columns will be prefixed with 'user.'
    Overlapping columns will be suffixed with '.lookup_users'
```

#### Import

```
%load_ext taegis_magic

from taegis_magic.pandas.subjects import lookup_users
```

#### Rules Example

```
%taegis rules suppression --kind tenant --assign rules
```

```
lookup_user_rules = rules.pipe(lookup_users, region="charlie", user_id_columns=['user.id'])
lookup_user_rules[[
    'id', 
    'user.id,'
    'user.id.user.email',
    'user.id.user.given_name',
]]
```

#### Clients Example

```
%taegis clients search --assign clients
```

```
lookup_users_clients = clients.pipe(lookup_users, region="charlie", user_id_columns=['created_by', 'updated_by'])
lookup_users_clients[[
    'client_id',
    'created_by',
    'created_by.user.email',
    'created_by.user.given_name',
    'created_by.user.family_name',
    'updated_by',
    'updated_by.user.email',
    'updated_by.user.given_name',
    'updated_by.user.family_name',
]]
```

#### Users Example

```
%taegis users current-user --assign current_user
```

```
lookup_users_current_user = current_user.pipe(lookup_users, region="charlie", user_id_columns=['updated_by'])
lookup_users_current_user[[
    'id',
    'updated_by',
    'updated_by.user.email',
    'updated_by.user.given_name',
    'updated_by.user.family_name',
]]
```
