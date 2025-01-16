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
