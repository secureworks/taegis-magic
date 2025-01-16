# Taegis Magic

## Assets

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [Assets](notebooks/Assets.ipynb)

### Lookup Assets

Endpoint asset information can be mapped backed to alerts or events based on the `host_id` field.

This pipe function does create an API call; set the `region` keyword argument in the pipe to query to correct Taegis region.

```python
%load_ext taegis_magic

from taegis_magic.pandas.alerts import inflate_raw_events
from taegis_magic.pandas.assets import lookup_assets
```

```python
%%taegis alerts search --assign alerts
FROM alert
EARLIEST=-1d | head 5
```

```python
alerts = alerts.pipe(inflate_raw_events).pipe(lookup_assets, region="US1")
```
