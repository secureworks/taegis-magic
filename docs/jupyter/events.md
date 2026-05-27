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
This section describes `DataFrame` pipe functions that take in a `DataFrame` with `process` table info and return a new `DataFrame` that contains correlated data or other event info. 

Please note that the number of rows returned by a `process` pipe function could be greater than the number of rows in the input `DataFrame` 
due to there sometimes being a 1:many relationship between the `process` table info and the table that gets queried as part of the pipe 
function.

### Correlation Functions
Process correlation functions typically have the following function signature: 

```
def process_correlate_{target_table}(
    df: pd.DataFrame,
    *,
    region: str,
    tenant_id: str,
    process_column: Optional[str] = "process_correlation_id",
    earliest: Optional[str] = "1d"
) -> pd.DataFrame:
```


Process correlation functions are effectively SQL left joins between the input `DataFrame` and `target_table` (e.g. for `process_correlate_auth` function `target_table` refers to `auth` table) based on the `process_column` (default value being `process_correlation_id`) values in the input `DataFrame`. What this means is that `process_column` values are taken from the input `DataFrame`, effectively a SELECT * FROM `target_table` where `target_table.process_correlation_id` = `DataFrame.process_column`  is executed, then whatever is returned from that query is merged into the input `DataFrame` and that is then returned (as a brand new `DataFrame`). Any columns in the returned `DataFrame` that come from the `target_table` are prefixed with `target_table`. 

Example is below: 

    >>> import pandas as pd
    >>> # Here df is the input DataFrame
    >>> df = pd.DataFrame({"process_correlation_id": ["host123:1234:56789", 1, "host123:1234:56789"]})
    >>> df
       process_correlation_id
    0      host123:1234:56789
    1                       1
    2      host123:1234:56789
    >>> result = df.pipe(process_correlate_{target_table}, region="us1", tenant_id="12345")
    >>> result
       process_correlation_id  target_table.host_id  target_table.processcorrelationid.pid  target_table.processcorrelationid.timewindow  target_table.process_correlation_id  ...
    0      host123:1234:56789          host123                        1234:56789                                      NaN              host123:1234:56789  ...
    1                       1              NaN                               NaN                                      NaN                             NaN  ...
    2      host123:1234:56789          host123                              1234                                    56789              host123:1234:56789  ...



#### process_correlate_auth

Correlation function to correlate process data with `auth` data (`auth` is the `target_table` here). Effectively does a left join between `process` and `auth` table based on `process_correlation_id`. Function first does a SELECT * from `auth` table where `process_correlation_id` is equal to the `process_correlation_id` values in input `DataFrame`, then merges those results into the input `DataFrame` and returns the result as a new `DataFrame`.

#### process_correlate_http

Correlation function to correlate process data with `http` data (`http` is the `target_table` here). Effectively does a left join between `process` and `http` table based on `process_correlation_id`. Function first does a SELECT * from `http` table where `process_correlation_id` is equal to the `process_correlation_id` values in input `DataFrame`, then merges those results into the input `DataFrame` and returns the result as a new `DataFrame`.

#### process_correlate_netflow

Correlation function to correlate `process` data with `netflow` data (netflow is the `target_table` here). Effectively does a left join between `process` and `netflow` table based on `process_correlation_id`. Function first does a SELECT * from `netflow` table where `process_correlation_id` is equal to the `process_correlation_id` values in input `DataFrame`, then merges those results into the input `DataFrame` and returns the result as a new `DataFrame`.

The correlation function for `netflow` events is a unique case. This is because there is not an equivalent `process_correlation_id` column
in the `netflow` table. 

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

Pivot functions have the following signature: 
```python
def process_pivot_func(
    df: pd.DataFrame,
    *,
    region: str,
    tenant_id: str,
    pivot_map: Optional[Mapping[str, str]] = None,
    earliest: Optional[str] = "1d"
) -> pd.DataFrame:
```


Each pivot function expects an input `DataFrame` with aggregate `process` data such as one the shown below: 

```python
>>> input_df
                                    host_id      sensor_type  count
    0  550e8400-e29b-41d4-a716-446655440001  ENDPOINT_SOPHOS    100
    1  550e8400-e29b-41d4-a716-446655440002  ENDPOINT_TAEGIS    200
    2  550e8400-e29b-41d4-a716-446655440003         FIREWALL     50
```

The pivot function will then query the table to the pivot to (i.e. pivot table) with WHERE clauses that are based off of the column names and their values from the input `DataFrame`. For example, for the `input_df` example above, it would generate the query below and return the results from the query as a `DataFrame`. 

```sql
FROM {pivot_table}
WHERE
    (host_id = '550e8400-e29b-41d4-a716-446655440001' AND sensor_type = 'ENDPOINT_SOPHOS') or 
    (host_id = '550e8400-e29b-41d4-a716-446655440002' AND sensor_type = 'ENDPOINT_TAEGIS') or 
    (host_id = '550e8400-e29b-41d4-a716-446655440003' AND sensor_type = 'FIREWALL')
EARLIEST=-1d
```

Notice how in this particular case the number of total where clauses = the number of rows in the `input_df`. 

Each time a pivot function is called, by default, the where clauses generated are determined by the matches between the `pivot_columns` passed to the pivot function and the column names in the input `DataFrame`. Each pivot function already has a predetermined list of `pivot_columns` which are a list of columns that both the `process` table and the pivot table have in common. Please see notes for each process pivot function below to see what those `pivot_columns` are. 

Building off of the previous `input_df` example, consider if it had another column named `other_column`: 

```python
>>> input_df
                                    host_id      sensor_type  other_column count
    0  550e8400-e29b-41d4-a716-446655440001  ENDPOINT_SOPHOS         delta   100
    1  550e8400-e29b-41d4-a716-446655440002  ENDPOINT_TAEGIS         sigma   200
    2  550e8400-e29b-41d4-a716-446655440003         FIREWALL       foxtrot   300
>>> # Example of what pivot_columns may be
>>> from taegis_magic.pandas.process import NETFLOW_PIVOT_COLUMNS
>>> print(NETFLOW_PIVOT_COLUMNS) 
    ["host_id", "sensor_id", "sensor_tenant", "sensor_type", "tenant_id"]
>>> from taegis_magic.pandas.process import process_pivot_netflow
>>> # Actually call pipe pivot function
>>> input_df.pipe(process_pivot_netflow, region="charlie", tenant_id="11063")
```

The call to `input_df.pipe(process_pivot_netflow, region="charlie", tenant_id="11063")` would generate a query that is the same as the one above because the column `other_column` is not part of the `NETFLOW_PIVOT_COLUMNS`. Since `host_id` and `sensor_type` are part of `NETFLOW_PIVOT_COLUMNS` and are in the `input_df` they are part of the query's WHERE clauses. 

If one would like to override the pivot columns, a value must be provided for the `pivot_map` argument which is `None` by default. The `pivot_map` must have keys that are the same as the name of the columns in the input `DataFrame` and the corresponding values are the names of the columns in the target pivot table. 

```python
>>> input_df
                                    host_id      sensor_type  other_column count
    0  550e8400-e29b-41d4-a716-446655440001  ENDPOINT_SOPHOS         delta   100
    1  550e8400-e29b-41d4-a716-446655440002  ENDPOINT_TAEGIS         sigma   200
    2  550e8400-e29b-41d4-a716-446655440003         FIREWALL       foxtrot   300

>>> from taegis_magic.pandas.process import process_pivot_netflow
>>> # Create a pivot_map
>>> pivot_map={"sensor_type":"t_sensor"}
>>> # Actually call pipe pivot function with pivot_map
>>> input_df.pipe(process_pivot_netflow, region="charlie", tenant_id="11063", pivot_map=pivot_map)
```

The call to `input_df.pipe(process_pivot_netflow, region="charlie", tenant_id="11063", pivot_map=pivot_map)` above would generate the following query: 


```sql
FROM netflow
WHERE
    (t_sensor = 'ENDPOINT_SOPHOS') or 
    (t_sensor = 'ENDPOINT_TAEGIS') or 
    (t_sensor = 'FIREWALL')
EARLIEST=-1d
```

Notice how in `input_df` the name of the column was `sensor_type` but in the where clauses it got remapped to `t_sensor`. It is important to note that ONLY columns specified in the `pivot_map` will be included in the WHERE clauses. 


#### process_pivot_auth
Pivot aggregate process data into non-aggregate netflow event rows. 
Static `pivot_columns` can be shown by executing the code below:
```python
from taegis_magic.pandas.process import AUTH_PIVOT_COLUMNS
print(AUTH_PIVOT_COLUMNS) 
```

#### process_pivot_http
Pivot aggregate process data into non-aggregate http event rows.
Static `pivot_columns` can be shown by executing the code below:
```python
from taegis_magic.pandas.process import HTTP_PIVOT_COLUMNS
print(HTTP_PIVOT_COLUMNS) 
```

#### process_pivot_netflow
Pivot aggregate process data into non-aggregate http event rows.
Static `pivot_columns` can be shown by executing the code below:
```python
from taegis_magic.pandas.process import NETFLOW_PIVOT_COLUMNS
print(NETFLOW_PIVOT_COLUMNS) 
```

#### process_pivot_detectionfinding
Pivot aggregate process data into non-aggregate detectionfinding event rows.
Static `pivot_columns` can be shown by executing the code below:
```python
from taegis_magic.pandas.process import DETECTIONFINDING_PIVOT_COLUMNS
from taegis_magic.pandas.process import process_pivot_detectionfinding
print(DETECTIONFINDING_PIVOT_COLUMNS)
```