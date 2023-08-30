# Taegis Magic

## Alerts

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [Alerts](notebooks/Alerts.ipynb)

### Search

```python
%load_ext taegis_magic
```

```python
%%taegis alerts search --assign alerts
FROM alert
EARLIEST=-1d | head 5
```

### Convert Timestamps

Timestamps returned by the Alerts API are in Unix epoch timestamp integers.  These can be converted into human readable timestamps with the pipe function `convert_alert_timestamps`.  This creates a new set of columns with `taegis_magic.` prepended to the timestamp related columns.

```python
from taegis_magic.pandas.alerts import convert_alert_timestamps
```

```python
convert_timestamps = alerts.pipe(convert_alert_timestamps)

convert_timestamps[[
    'metadata.created_at.seconds',
    'taegis_magic.metadata.created_at.seconds',
    'metadata.inserted_at.seconds',
    'taegis_magic.metadata.inserted_at.seconds',
    'metadata.began_at.seconds',
    'taegis_magic.metadata.began_at.seconds',
    'metadata.ended_at.seconds',
    'taegis_magic.metadata.ended_at.seconds',
]]
```

<table border="1" class="dataframe">  <thead>  <tr style="text-align: right;">  <th></th>  <th>metadata.created_at.seconds</th>  <th>taegis_magic.metadata.created_at.seconds</th>  <th>metadata.inserted_at.seconds</th>  <th>taegis_magic.metadata.inserted_at.seconds</th>  <th>metadata.began_at.seconds</th>  <th>taegis_magic.metadata.began_at.seconds</th>  <th>metadata.ended_at.seconds</th>  <th>taegis_magic.metadata.ended_at.seconds</th>  </tr>  </thead>  <tbody>  <tr>  <th>0</th>  <td>1691694875</td>  <td>2023-08-10T19:14:35Z</td>  <td>1691694878</td>  <td>2023-08-10T19:14:38Z</td>  <td>1691694408</td>  <td>2023-08-10T19:06:48Z</td>  <td>1691694873</td>  <td>2023-08-10T19:14:33Z</td>  </tr>  <tr>  <th>1</th>  <td>1691694874</td>  <td>2023-08-10T19:14:34Z</td>  <td>1691694877</td>  <td>2023-08-10T19:14:37Z</td>  <td>1691680223</td>  <td>2023-08-10T15:10:23Z</td>  <td>1691694868</td>  <td>2023-08-10T19:14:28Z</td>  </tr>  <tr>  <th>2</th>  <td>1691694874</td>  <td>2023-08-10T19:14:34Z</td>  <td>1691694878</td>  <td>2023-08-10T19:14:38Z</td>  <td>1691694871</td>  <td>2023-08-10T19:14:31Z</td>  <td>1691694871</td>  <td>2023-08-10T19:14:31Z</td>  </tr>  <tr>  <th>3</th>  <td>1691694874</td>  <td>2023-08-10T19:14:34Z</td>  <td>1691694877</td>  <td>2023-08-10T19:14:37Z</td>  <td>1691694871</td>  <td>2023-08-10T19:14:31Z</td>  <td>1691694871</td>  <td>2023-08-10T19:14:31Z</td>  </tr>  <tr>  <th>4</th>  <td>1691694874</td>  <td>2023-08-10T19:14:34Z</td>  <td>1691694877</td>  <td>2023-08-10T19:14:37Z</td>  <td>1691694801</td>  <td>2023-08-10T19:13:21Z</td>  <td>1691694869</td>  <td>2023-08-10T19:14:29Z</td>  </tr>  </tbody></table>

The format of the timestamp can be changed using the string time identifiers defined under [strftime](https://docs.python.org/3/library/time.html#time.strftime).

```
convert_timestamps = alerts.pipe(convert_alert_timestamps, format_="%Y-%m-%d")

convert_timestamps[[
    'metadata.created_at.seconds',
    'taegis_magic.metadata.created_at.seconds',
    'metadata.inserted_at.seconds',
    'taegis_magic.metadata.inserted_at.seconds',
    'metadata.began_at.seconds',
    'taegis_magic.metadata.began_at.seconds',
    'metadata.ended_at.seconds',
    'taegis_magic.metadata.ended_at.seconds',
]]
```

<table border="1" class="dataframe"> <thead> <tr style="text-align: right;"> <th></th> <th>metadata.created_at.seconds</th> <th>taegis_magic.metadata.created_at.seconds</th> <th>metadata.inserted_at.seconds</th> <th>taegis_magic.metadata.inserted_at.seconds</th> <th>metadata.began_at.seconds</th> <th>taegis_magic.metadata.began_at.seconds</th> <th>metadata.ended_at.seconds</th> <th>taegis_magic.metadata.ended_at.seconds</th> </tr> </thead> <tbody> <tr> <th>0</th> <td>1691694875</td> <td>2023-08-10</td> <td>1691694878</td> <td>2023-08-10</td> <td>1691694408</td> <td>2023-08-10</td> <td>1691694873</td> <td>2023-08-10</td> </tr> <tr> <th>1</th> <td>1691694874</td> <td>2023-08-10</td> <td>1691694877</td> <td>2023-08-10</td> <td>1691680223</td> <td>2023-08-10</td> <td>1691694868</td> <td>2023-08-10</td> </tr> <tr> <th>2</th> <td>1691694874</td> <td>2023-08-10</td> <td>1691694878</td> <td>2023-08-10</td> <td>1691694871</td> <td>2023-08-10</td> <td>1691694871</td> <td>2023-08-10</td> </tr> <tr> <th>3</th> <td>1691694874</td> <td>2023-08-10</td> <td>1691694877</td> <td>2023-08-10</td> <td>1691694871</td> <td>2023-08-10</td> <td>1691694871</td> <td>2023-08-10</td> </tr> <tr> <th>4</th> <td>1691694874</td> <td>2023-08-10</td> <td>1691694877</td> <td>2023-08-10</td> <td>1691694801</td> <td>2023-08-10</td> <td>1691694869</td> <td>2023-08-10</td> </tr> </tbody></table>

### Events

Taegis Magic by default pulls event data alongside alert data.  See the [Taegis SDK for Python](https://github.com/secureworks/taegis-sdk-python/blob/main/docs/extending_the_sdk.md).

By default, this data is grouped into the `event_data` field in alerts.  In order to set the event data side by side with the alert data, use the `inflate_event_data` pipe function.

Alerts may have been created from multiple events.  This function will expand the DataFrame so each event will be along side the alert data (potentially duplicating alert data) and set the `event_data` columns side by side with the alert data in columns preprended with `event_data.`.  These columns will be dynamic per underlying event type.

```python
from taegis_magic.pandas.alerts import inflate_raw_events
```

We can see the new columns using the following `set` functions.

```python
with_events = alerts.pipe(inflate_raw_events)

set(with_events.columns).difference(set(alerts.columns))
```

This output may differ based on your alert set.

```python
{'event_data.alerts_resource_id',
 'event_data.commandline',
 'event_data.direction',
 'event_data.enrichSummary',
 'event_data.event_time_fidelity',
 'event_data.event_time_usec',
 'event_data.event_type',
 ...
}
```

**Note**: Results are truncated for readability.

### Alerts Aggregration

Taegis Alerts search allows for aggregation functions.  This can be useful for prioritizing which alerts to triage or report.  It can also be useful to dig into the alert specifics on a filtered data set.

This function does require exact matches on the selected fields.  This may cause issues on fields that may require partial or rounded matches.

This pipe function does create an API call; set the `region` keyword argument in the pipe to query to correct Taegis region.

```python
%%taegis alerts search --assign aggregate_alerts
FROM alert
EARLIEST=-1d | aggregate count by metadata.title | head 5
```

```python
from taegis_magic.pandas.alerts import get_alerts_from_aggregation
``` 

```python
alerts = aggregate_alerts.pipe(get_alerts_from_aggregation)
```

Function option:
* `region` [Taegis region to query]
* `tenant` [Tenant ID if a specific tenant is needed]
* `earliest` [See Taegis Time Ranges]
* `latest` [See Taegis Time Ranges]
* `limit` [Maximum number of alerts to return]

```python
alerts = aggregate_alerts.pipe(
    get_alerts_from_aggregation,
    region="charlie",
    tenant="xxxxx",
    earliest="-2d",
    latest="-1d",
    limit=50,
)
```

[Taegis Time Ranges](https://docs.ctpx.secureworks.com/search/querylanguage/advanced_search/#time-ranges)

### Third Party Details

Taegis allows for the ingest and handling of alerts from third parties.  Alerts have a section called `third_party_details` to allow access raw data from ingest.  We can set this data side by side with the pipe function `inflate_third_party_details`.

```python
from taegis_magic.pandas.alerts import inflate_third_party_details
```

You can test the column difference with the following:

```python
tpd_alerts = alerts.pipe(inflate_third_party_details)

set(tpd_alerts.columns).difference(set(alerts.columns))
```

```python
{'third_party_details.AccountDomain',
 'third_party_details.AccountName',
 'third_party_details.AccountObjectId',
 'third_party_details.AccountSid',
 'third_party_details.AlertId',
 ...
}
```

**Note**: Results are truncated for readability.

### Alert Severity

Taegis sets the alert severity as a decimal number.  We can translate this to a rounded number and human readable category with the pipe function `severity_rounded_and_category`.

```python
from taegis_magic.pandas.alerts import severity_rounded_and_category
```

```python
transformed_alerts = alerts.pipe(severity_rounded_and_category)

transformed_alerts[["metadata.severity", "taegis_magic.severity", "taegis_magic.severity_category"]]
```

<table border="1" class="dataframe">  <thead>    <tr style="text-align: right;">      <th></th>      <th>metadata.severity</th>      <th>taegis_magic.severity</th>      <th>taegis_magic.severity_category</th>    </tr>  </thead>  <tbody>    <tr>      <th>0</th>      <td>0.1</td>      <td>0.1</td>      <td>Informational</td>    </tr>    <tr>      <th>1</th>      <td>0.2</td>      <td>0.2</td>      <td>Low</td>    </tr>    <tr>      <th>2</th>      <td>0.5</td>      <td>0.5</td>      <td>Medium</td>    </tr>    <tr>      <th>3</th>      <td>0.4</td>      <td>0.4</td>      <td>Medium</td>    </tr>    <tr>      <th>4</th>      <td>0.2</td>      <td>0.2</td>      <td>Low</td>    </tr>  </tbody></table>

### Provide Feedback / Resolution

Users can triage alerts and provide [resolution feedback](https://docs.ctpx.secureworks.com/alerts/resolve_alerts/) using the pipe function `provide_feedback`.  This function is tenant aware and doesn't need to be specified.

```python
from taegis_magic.pandas.alerts import provide_feedback
from taegis_sdk_python.services.alerts.types import ResolutionStatus
```

```python
alerts.pipe(provide_feedback, environment="charlie", status=ResolutionStatus.FALSE_POSITIVE, reason="my resolution reason")
```

### Pretty Detector Names

The Taegis XDR UI translates the detector names.  Taegis Magic can as well with the pipe function `normalize_creator_name`!  This function does send an API call and may require a region to be specified.

```python
from taegis_magic.pandas.alerts import normalize_creator_name
```

```python
alerts = alerts.pipe(normalize_creator_name, region="charlie")
alerts["taegis_magic.creator.display_name"]
```
