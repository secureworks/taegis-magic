# Taegis Magic

## Explain

**Note**: Each block represents a Jupyter Notebook cell.

### Explain Command Line Commands

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
explained_events = events.pipe(get_command_line_explanation, region=region, tenant_id=tenant)
```

`explained_events`, a `DataFrame`, is a new DataFrame that contains the input DataFrame in addition to 3 new columns, `command`, `explanation`, `event`. The 3 new columns would look like the following: 

```python
command | explanation | event
C:\Windows\system32\WerFault.exe -u -p 5576 | - Most likely CLI environment: Windows Command... |  event://priv:scwx.process:11063:1779911715105
```

If invalid event_ids (e.g. they aren't real event_ids, they don't exist, etc.) are included in the input `DataFrame`, the 3 new columns will still be added but instead a message in the returned `DataFrame` will indicate that the commands and explanations could not be generated. 


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
explained_events = events.pipe(get_event_explanation, region=region, tenant_id=tenant)
```

`explained_events`, a `DataFrame`, is a new DataFrame that contains the input DataFrame in addition to 3 new columns, `error`, `explanation`, `event`. The 3 new columns would look like the following: 

```python
command | explanation | event
None | - Most likely CLI environment: Windows Command... |  event://priv:scwx.process:11063:1779911715105
```
