# Taegis Magic

## Events

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [Events](notebooks/Events.ipynb)

```python
%load_ext taegis_magic
```

```python
%%taegis events search --assign events
FROM process
EARLIEST=-1h | head 5
```

**Note:** The Event Schema provided is an example, this data returned will be dependant on integrations configured with Taegis and normalized into individual schemas.

### Convert Timestamps

Timestamps returned by the Events API are in Unix epoch timestamp integers.  These can be converted into human readable timestamps with the pipe function `convert_event_timestamps`.  This creates a new set of columns with `taegis_magic.` prepended to the timestamp related columns.

The format of the timestamp can be changed using the string time identifiers defined under [strftime](https://docs.python.org/3/library/time.html#time.strftime).

```python
from taegis_magic.pandas.events import convert_event_timestamps
```

```python
convert_timestamps = events.pipe(convert_event_timestamps)
convert_timestamps[[
    'event_time_usec',
    'taegis_magic.event_time_usec',
    'ingest_time_usec',
    'taegis_magic.ingest_time_usec'
]]
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>event_time_usec</th>
      <th>taegis_magic.event_time_usec</th>
      <th>ingest_time_usec</th>
      <th>taegis_magic.ingest_time_usec</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1736968618000000</td>
      <td>2025-01-15T19:16:58Z</td>
      <td>1736968682000000</td>
      <td>2025-01-15T19:18:02Z</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1736968618000000</td>
      <td>2025-01-15T19:16:58Z</td>
      <td>1736968682000000</td>
      <td>2025-01-15T19:18:02Z</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1736968618000000</td>
      <td>2025-01-15T19:16:58Z</td>
      <td>1736968682000000</td>
      <td>2025-01-15T19:18:02Z</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1736968618000000</td>
      <td>2025-01-15T19:16:58Z</td>
      <td>1736968682000000</td>
      <td>2025-01-15T19:18:02Z</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1736968618000000</td>
      <td>2025-01-15T19:16:58Z</td>
      <td>1736968682000000</td>
      <td>2025-01-15T19:18:02Z</td>
    </tr>
  </tbody>
</table>

## Inflate Original Data

Taegis stores the original log as `original_data` in the event.  When this log is JSON parsable, `inflate_original_data` will present the log side by side with the normalized event data with the prepended columns `original_data.`.

```python
from taegis_magic.pandas.events import convert_event_timestamps
```

```python
inflate_original_data_df = events.pipe(inflate_original_data)
inflate_original_data_df[column for column in inflate_original_data_df.columns if column.startswith("original_data.")]
```

## Inflate Schema Keys

Taegis Event Schema Keys can be queried with `taegis events schema`.  These keys are in the format of `scwx.schema.key`.  The schema and key can be cleaned and separated with the `inflate_schema_keys` pipe function prepended with `taegis_magic.`.

```python
from taegis_magic.pandas.events import inflate_schema_keys
```

```python
%taegis events schema --type auth --assign auth_schema
```

```
inflated_schema = auth_schema.pipe(inflate_schema_keys)
inflated_schema
```

<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>key</th>
      <th>taegis_magic.schema</th>
      <th>taegis_magic.key</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>scwx.auth.home_directory</td>
      <td>auth</td>
      <td>home_directory</td>
    </tr>
    <tr>
      <th>1</th>
      <td>scwx.auth.subject_domain_user_id</td>
      <td>auth</td>
      <td>subject_domain_user_id</td>
    </tr>
    <tr>
      <th>2</th>
      <td>scwx.auth.process_create_time_usec</td>
      <td>auth</td>
      <td>process_create_time_usec</td>
    </tr>
    <tr>
      <th>3</th>
      <td>scwx.auth.process_file_hash.sha256</td>
      <td>auth</td>
      <td>process_file_hash.sha256</td>
    </tr>
    <tr>
      <th>4</th>
      <td>scwx.auth.target_domain_name</td>
      <td>auth</td>
      <td>target_domain_name</td>
    </tr>
  </tbody>
</table>

## Process Pipe Functions
This section describes `DataFrame` pipe functions that take in a `DataFrame` with `process` info and return correlated or other event info such as `netflow`, `http`, `auth`. 
### Correlation Functions

The `process_correlate_netflow` function, as shown below, is a pipe function that accepts a `Pandas` `DataFrame` with `process` event information and finds `netflow` events that are correlated based on each `process` event's `process_corelation_id`. A new `DataFrame` is returned that will include the columns from the input `DataFrame` as well as columns from the `netflow` table and will likely contain more rows that the input `DataFrame` as there is a 1:many relationship between `process:netflow`. 

```python
from taegis_magic.pandas.process import process_correlation_netflow
```

A `process_correlation_id` has the structure: `{host_id}:{pid}:{id.time_window}`. `netflow` events do not have a full `process_correlation_id` as `process` events do but do have the original components, `host_id`, `processcorrelationid.pid`, and `processcorrelation.timewindow`. `netflow` events can sometimes have different structures for `processcorrelationid` as shown below. The pipe function takes these differences into account. 

```json
// pid has structure of {pid}:{id.time_window}, no value for timewindow
{
  "processcorrelationid":
        {
            "pid": "14345:73252035284761978",
            "timewindow": ""
        },
  "host_id": "c57e07c5bf44-4af3-4af3-4af3-4af34af3"
}
```

```json
// pid has structure of {pid}, and timewindow is not empty. 
{
  "processcorrelationid":
        {
            "pid": "14345",
            "timewindow": "73252035284761978"
        },
  "host_id": "c57e07c5bf44-4af3-4af3-4af3-4af34af3"
}
```


### Pivot Functions

There are several pivot functions that "pivot" from `process` -> some other type of event such as `netflow`, `http`, `auth`, etc.

#### _process_pivot_base_func
The `_process_pivot_base_func` function, as shown below, is a pipe function that accepts a `Pandas` `DataFrame` with aggregate `process` event information and finds events from the table to pivot to (i.e. pivot table) by creating a query against that table with filters (i.e. WHERE clauses) based on the column names of the input `DataFrame` and `pivot_columns` parameters.  A new `DataFrame` is returned that contains events from the query. 

```python
from taegis_magic.pandas.process import _process_pivot_base_func
```

`_process_pivot_base_func` has the following function signature: 

```python
def _process_pivot_base_func(
    df: pd.DataFrame,
    region: str,
    tenant_id: str,
    query_template: str,
    table: str,
    pivot_columns: list[str],
    earliest: str
) -> pd.DataFrame:
```

The function parses the names of the columns in the input `df`, checks to see which ones exist in the `pivot_columns` list, and then builds the filters based on the matches between the two. `pivot_columns` is a list of columns that both the `process` table and the pivot table have in common. Note that this list does not include all column names that are shared between the two tables, but ones that are most likely used to aggregate `process` information and of course also exist in the pivot table. 

The input `DataFrame` should look something like: 
```
                                host_id      sensor_type non_matching_column  count
0  550e8400-e29b-41d4-a716-446655440001  ENDPOINT_SOPHOS               alpha    100
1  550e8400-e29b-41d4-a716-446655440002  ENDPOINT_TAEGIS                beta    200
2  550e8400-e29b-41d4-a716-446655440003         FIREWALL               gamma     50
```

`_process_pivot_base_func` parses the columns and generates a query against the pivot table to that looks like:

```
FROM {table_to_pivot_to}
  WHERE
    (host_id = '550e8400-e29b-41d4-a716-446655440001' AND sensor_type = 'ENDPOINT_SOPHOS') or 
    (host_id = '550e8400-e29b-41d4-a716-446655440002' AND sensor_type = 'ENDPOINT_TAEGIS') or 
    (host_id = '550e8400-e29b-41d4-a716-446655440003' AND sensor_type = 'FIREWALL')
  EARLIEST=-1d
```

Note how the `WHERE` clauses do not include the `non_matching_column` from the input `DataFrame` as that column was not part of the `pivot_columns` list in this example. 


#### _process_pivot_with_map

`_process_pivot_with_map` works similarly to `_process_pivot_base_func` with the addition of a `pivot_map` parameter as shown below. `pivot_map` contains keys that are column names in the input DataFrame and the corresponding values are column names in the table to pivot to. Therefore, the pivot_map remaps DataFrame column names to target table column names in WHERE clauses. 

```python
def _process_pivot_with_map(
    df: pd.DataFrame,
    region: str,
    tenant_id: str,
    query_template: str,
    table: str,
    pivot_columns: list[str],
    pivot_map: Optional[Mapping[str, str]],
    earliest: str
) -> pd.DataFrame:
```

This function allows for additional "customization" to overcome the "limitations" of `_process_pivot_base_func`. `_process_pivot_base_func` only works with a static list of columns that must exist both in the `process` table and the pivot table. If there is schema evolution, a mismatch in column names between `process` and target table with similar data, custom column names, etc. then the `_process_pivot_base_func` is not as useful. This function helps overcome these limitations without adding new code. 

In fact, all process pivot functions are wrappers of `_process_pivot_with_map`. By default, `pivot_map` is `None` and if it is indeed empty then `_process_pivot_base_func` is called. If `pivot_map` is non-empty then `_process_pivot_with_map` is used instead. Only columns specified in the `pivot_map` will be used in the WHERE clauses. 



The following example demonstrates what the input DataFrame is expected to look like, what `pivot_map` parameter may look like,
and then shows what kind of query the function will execute based on the contents of the input DataFrame and `pivot_map`. 
This is just meant for demonstration purposes, the column names may or may not be real. 
```python
>>> # Remember, input_df contains aggregate process info. 
>>> input_df
                                host_id      sensor_type                 env  count
0  550e8400-e29b-41d4-a716-446655440001  ENDPOINT_SOPHOS               alpha    100
1  550e8400-e29b-41d4-a716-446655440002  ENDPOINT_TAEGIS                beta    200
2  550e8400-e29b-41d4-a716-446655440003         FIREWALL               gamma     50
>>> # Example value of `pivot_map` parameter
>>> print(pivot_map)
{
    "sensor_type": "sensor_type",
    "env": "region"
}
>>> # Example of calling a pipe pivot function that uses `pivot_map
>>> agg_process_events.pipe(process_pivot_netflow, region=region, tenant_id=tenant_id, pivot_map=pivot_map, earliest="30d")
```


For the `input_df` dataframe above, the generated query would look like:

    FROM netflow
    WHERE
        (sensor_type = 'ENDPOINT_SOPHOS' AND env = 'alpha') or 
        (sensor_type = 'ENDPOINT_TAEGIS' AND env = 'beta') or 
        (sensor_type = 'FIREWALL' AND env = 'gamma')
    EARLIEST=-30d


Notice how the `pivot_map` did not include `host_id` and therefore `host_id` was not included in the query. In addition, notice how
the values for `env` column got remapped to `region` in the query WHERE clauses.  


When using `_process_pivot_with_map`, one would create a wrapper function called `process_pivot_{name_of_pivot_table}`, and this wrapper function would have a signature that looks like: 

```python
def process_pivot_{name_of_pivot_table}(
    df: pd.DataFrame,
    *,
    region: str,
    tenant_id: str,
    pivot_map: Optional[Mapping[str, str]] = None,
    earliest: Optional[str] = "1d"
) -> pd.DataFrame:
  return _process_pivot_with_map(df, region, tenant_id, {query_template}, {name_of_pivot_table}, {pivot_columns}, {pivot_map}, earliest)
```