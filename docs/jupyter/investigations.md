# Taegis Magic

## Investigations

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [Investigations](notebooks/Investigations.ipynb)

### Evidence

Taegis Magic can help create investigations tying in with the evidence portion in the Taegis XDR UI.  There are three key pieces of information that Taegis Magic manage: `alerts`, `events`, and `search_queries`.

Staging evidence deduplicates entries, so if evidence is repeated in multiple data sources, they will not be duplicated within the evidence.

If evidence is staged using multiple tenants, the evidence will remain separated by tenant at investigation creation to reduce Cross Client Data Pollination.

Evidence staged in notebooks is kept in memory; if the notebook is restarted, then staged evidence is reset. To save between notebook runs, include the `--database [filename]` option may be included to save the evidence to disk.

#### Alerts

Taegis Alerts are staged by their `id` field.  Alert DataFrames may be filtered by row and columns, but for `%taegis investigations evidence stage` to work, it needs to retain at least the `id` column.

The following is meant to showcase the functionality and not to instruct how to conduct investigations.

```python
%%taegis alerts search --track --assign alerts_dataframe
FROM alert
EARLIEST=-1d | head 5
```

```python
%taegis investigations evidence stage alerts alerts_dataframe
```

```python
%taegis investigations evidence show
```

#### Events

Taegis events are staged by their `id` field.  Event DataFrames may be filtered by row and columns, but for `%taegis investigations evidence stage` to work, it needs to retain at least the `resource_id` column.

The following is meant to showcase the functionality and not to instruct how to conduct investigations.  The event type may need updated per event types that are ingested for your monitored environment. 

```python
%%taegis events search --track --assign events_dataframe
FROM process
EARLIEST=-1d | head 5
```

```python
%taegis investigations evidence stage events events_dataframe
```

```python
%taegis investigations evidence show
```

#### Search Queries

When running Alert or Event search queries, the option `--track` may be used to submit the query into the database.  Search queries can be configured to automatically be tracked using the `%taegis configure queries track --status yes`.  If this command is run in a notebook, the notebook will need to be restarted before changes take effect.

`%taegis investigations search-queries list` can be used to show what queries have been run (with some metadata).  

`%taegis investigations search-queries remove [id]` can be used to remove individual queries from the database.  This is useful when a query was tracked but does not provide value to the investigation.  

`%taegis investigations search-queries clear` can be used to reset the database to empty.  

`%taegis investigations search-queries stage` will move all the search queries under the investigation evidence and clear the query queue.  Search queries that are staged for investigations will be moved from `Search History` in the UI to `Saved Searches` at investigation creation time.

### Search

```
%load_ext taegis_magic
```

```
%%taegis investigations search --assign investigations_df
WHERE id = '<uuid>'
```

### Creation

Taegis investigations may be created from a Jupyter notebook.  The only requirements for a new Taegis investigation are a `--title` and `--key-findings`.  The key findings are read from a markdown file separate from the notebook.  The `%generate_report` [line magic](generate_report.md) may be used to convert the running notebook to markdown.  Evidence is read from the database at run time.  Evidence will be restricted to `NEW` investigations for a specified tenant.

The `--assignee-id` has a macro system to ease automation:

* `@me`: sets assignment to the calling user or client id
* `@partner`: sets assignment to the calling user's partner organization
* `@tenant`: sets assignment to the investigation's tenant organization
* Email addresses may be used instead of the user id as well

```
%taegis investigations create --title "Test Investigation" --key-findings "key_findings.md" --priorty LOW --type SECURITY_INVESTIGATION --status OPEN
```

### Append

Taegis investigation evidence may be modified through appending new evidence from a Jupyter notebook.

Appending alerts example:

```
%%taegis alerts search --track --assign alerts_df
FROM alert EARLIEST=-1d | head 5
```

```
%taegis investigations evidence stage alerts alerts_df --investigation-id "<uuid>"
```

Appending events example:

```
%%taegis events search --tenant 50530 --track --assign events_df
FROM cloudaudit EARLIEST=-1d | head 5
```

```
%taegis investigations evidence stage events events_df --investigation-id "<uuid>"
```

Appending search queries example:

```
%taegis investigations search-queries stage --investigation-id "<uuid>"
```

Evidence may be reviewed prior to modifying the investigation.  

```
%taegis investigations evidence show --investigation-id "<uuid>"
```

All staged evidence (alerts, events and search queries) assigned to investigation-id will be appened to the investigation evidence.  Evidence labeled as `NEW` in the investigation identifier may be used instead with the `--use-new` flag.  The `--investigation-id` will still be needed to append to the investigation.

```
%taegis investigations evidence append --investigation-id "<uuid>"
```

### Remove

Taegis investigation evidence may be modified through removing existing evidence from a Jupyter notebook.

#### Example

```
%load_ext taegis_magic

from taegis_magic.pandas.investigations import inflate_evidence
```

```
%%taegis investigations search --assign investigations_df
WHERE id = '<uuid>'
```

```
evidence = investigations.pipe(inflate_evidence).pipe(lookup_evidence, region="charlie")
evidence
```

```
sample = evidence.sample(n=5)
sample
```

```
%taegis investigations evidence stage all sample --investigation-id "<uuid>"
```

```
%taegis investigations evidence remove --investigation-id "<uuid>"
```

### Inflate Evidence

Investigation Evidence can be inflated from the investigation DataFrame.  This will generate a new DataFrame with the raw evidence identifiers.  Correlation of underlying information can be obtained with `lookup_evidence`.

```python
from taegis_magic.pandas.investigations import inflate_evidence
```

```
%load_ext taegis_magic
```

```
%%taegis investigations search --assign investigations_df
WHERE id = '<uuid>'
```

```
evidence = investigations_df.pipe(inflate_evidence)
```


### Lookup Evidence

Lookup Evidence will correlate the evidence identifiers to the corresponding Taegis API services for Alerts, Events and Search Queries.  The `region` input can be used to change region for the API calls.  Tenant context will be assigned from the input DataFrame's 'tenant_id` column.

```python
from taegis_magic.pandas.investigations import inflate_evidence, lookup_evidence
```

```
%load_ext taegis_magic
```

```
%%taegis investigations search --assign investigations_df
WHERE id = '<uuid>'
```

```python
evidence = investigations.pipe(inflate_evidence).pipe(lookup_evidence, region="charlie")
```
