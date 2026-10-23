# Taegis Magic

## Explain

Explain functions are used to get LLM generated explanations of specific data within a given `DataFrame`. Note, that these functions will generally append new columns to the `DataFrame` that is passed in to them where the columns will be prefixed with what is being explained. For example, if a function is for explaining what a command line command is doing, the added columns will be prefixed with `command_line.`. These prefixes are intended to make clear what the explanations are for. It is particularly useful when one pipes multiple explain functions together to get explanations for various pieces of data. 

**Note**: Each block represents a Jupyter Notebook cell.

### Explain Command Line 

For any type of event that contains command line information (e.g. process events) a LLM explanation of the command can be generated. 

This can be done by having a DataFrame that contains a column which contains event_ids. Typically, the event_id is equivalent to the `resource_id` of the event, as the `resource_id` is a full resource string that identifies the record.

To get this LLM explanation, one can do the following: 


```python
# First retrieve some events
%%taegis events search --assign events
FROM process
EARLIEST=-1h | head 5
```

```python
# Generate explanations.
from taegis_magic.pandas.explain import get_command_line_explanation
explained_events = events.pipe(get_command_line_explanation, region=region, tenant_id=tenant)
```

`explained_events`, a `DataFrame`, is a new DataFrame that contains the input DataFrame in addition to 3 new columns, `command_line.command`, `command_line.explanation`, `command_line.event`. The 3 new columns would look like the following: 

```python
command_line.command | command_line.explanation | command_line.event
C:\Windows\system32\WerFault.exe -u -p 5576 | - Most likely CLI environment: Windows Command... |  event://priv:scwx.process:11063:1779911715105
```

If invalid event_ids (e.g. they aren't real event_ids, they don't exist, etc.) are included in the input `DataFrame`, something goes wrong during the API call, etc. an empty `DataFrame` will be returned. 


### Explain Events

For any type of event, a LLM explanation of the event can be generated. 

This can be done by having a DataFrame that contains a column which contains event_ids. Typically, the event_id is equivalent to the `resource_id` of the event, as the `resource_id` is a full resource string that identifies the record.

To get this LLM explanation, one can do the following: 

```python
# First retrieve some events
%%taegis events search --assign events
FROM cloudaudit
EARLIEST=-1h | head 5
```

```python
# Generate explanations.
from taegis_magic.pandas.explain import get_event_explanation
explained_events = events.pipe(get_event_explanation, region=region, tenant_id=tenant)
```

`explained_events`, a `DataFrame`, is a new DataFrame that contains the input DataFrame in addition to 3 new columns, `event.error`, `event.explanation`, `event.event`. Note that `event.error` column will only appear if an error actually occurred. Under normal circumstances, `event.error` will not be present. The 3 new columns would look like the following: 

```python
event.error | event.explanation | event.event
None | - Most likely CLI environment: Windows Command... |  event://priv:scwx.process:11063:1779911715105
```

If invalid event_ids (e.g. they aren't real event_ids, they don't exist, etc.) are included in the input `DataFrame`, something goes wrong during the API call, etc. an empty `DataFrame` will be returned.