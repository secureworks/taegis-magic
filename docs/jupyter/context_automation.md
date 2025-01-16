# Taegis Magic

## Context Automation

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [ContextAutomation](notebooks/ContextAutomation.ipynb).

Taegis Magic provides some features to help with gathering related context on a series of alerts or events based on logical entity types (IPs, domains, hashes, users, hosts) within the Taegis platform and correlate it with [CTU TIPS](https://docs.ctpx.secureworks.com/dashboard/alert_triage_dashboard/#threat-intelligence-reports) reports to help with bulk analysis and identify threats quickly.

### Requirements

A DataFrame containing results from Alerts or Events search queries is required to start the Taegis Magic Context Automation process.  The `taegis alerts search` or `taegis events search` commands are available as starting points, but any method of converting Taegis Alerts or Event data into a DataFrame should suffice.

```
%%taegis alerts search --region $REGION --tenant $TENANT --assign alerts_dataframe
FROM alert
WHERE
    metadata.creator.detector.detector_id = 'app:event-filter' AND
    status = 'OPEN' AND
    severity >= 0.6 AND
    investigation_ids IS NULL
EARLIEST=-1d
```

### normalize_entities

This pipe function is only needed for Alerts DataFrames to normalize the entity values from the `entities.entities` column and categories them under the `@ip`, `@hash`, `@domain`, `@user` and `@host` [logical types](https://docs.ctpx.secureworks.com/search/builder/advanced_search/#logical-types).

This function returns a copy of the provided DataFrame with new columns: `taegis_magic.entities.field` (logical type from the values above) and `taegis_magic.entities.value` (string representPation of entity).

```python
entities_df = alerts_dataframe.pipe(normalize_entities)
```

### relate_entites

Relates entities to all their corresponding indicators.  An example would be: an alert has entities of types `@ip` (1 entity), `@domain` (2 entities) and `@user` (2 users):  The results would correlate `@ip` to have a list of 2 domains and 2 users, `@domain` would be split into 2 portions, each with 1 IP address and 2 users, and `@user` would each be correlated to 1 IP address and 2 domains.

This function does require columns `taegis_magic.entities.field` and `taegis_magic.entities.value` from `normalize_entities` to be present.


```python
entities_df = entities_df.pipe(relate_entities)
```

These results could then be filtered to a single GROUP_BY for evalutation.  The purpose of this result is to configure a notebook to look at alerts/events from a certain perspective, but easily change the configuration if a different perspective is required.

```python
entities_df = entities_df[
    entities_df["taegis_magic.entities.field"] == GROUP_BY
].reset_index(drop=True)
```

```python
entities_df[
    [
        "tenant.id",
        "metadata.title",
        "taegis_magic.entities.field",
        "taegis_magic.entities.value",
    ]
    + [
        column
        for column in entities_df.columns
        if column.startswith("@") and column != GROUP_BY
    ]
]
```

### generate_queries

This pipe function generates the context queries for searching related information within Taegis, such as open alerts, previously evaluated (or resolved) alerts, alerts already in investigations and related raw events.

This function may be configured with the following arguments to customize the time frame of the queries.  This is useful for automating analysis on past activity.

    * open_alerts_timeframe
    * resolved_alerts_timeframe,
    * investigations_timeframe
    * events_timeframe

For more information on Taegis time range formats, see:
[time ranges](https://docs.ctpx.secureworks.com/search/querylanguage/advanced_search/#time-ranges).

This function returns a copy of the provided DataFrame with new columns:
`taegis_magic.open_alerts_query`, `taegis_magic.resolved_alerts_query`, `taegis_magic.investigations_query`, `taegis_magic.events_query` which contains a CQL query string to help gather that specific context.

```python
entities_df = entities_df.pipe(
    generate_context_queries,
)
entities_df[
    [
        "taegis_magic.open_alerts_query",
        "taegis_magic.resolved_alerts_query",
        "taegis_magic.investigations_query",
        "taegis_magic.events_query",
    ]
]
```

### add_threat_intel

This pipe function takes a list of callables to correlate threat intelligence.  Taegis Magic provides `get_ti_pubs` as an interface for [CTU TIPS](https://docs.ctpx.secureworks.com/dashboard/alert_triage_dashboard/#threat-intelligence-reports) correlation from the Taegis platform, but a function may be created for any threat intelligence.  It is the author's responsibility to safeguard any API keys that may be needed to interface with 3rd party sources.

This function returns a copy of the provided DataFrame with new columns dependant on the list of callables provided:

* `get_ti_pubs` adds columns `tips.found` (bool) and `tips.publications` (List[ThreatPublication] from the Taegis SDK for Python library).  Useful for quickly identifying events that are related to CTU publications along with the title of the publication.

```python
# correlate threat intel
# new correlations can be added as a custom callable
for entity in entity_queries:
    for query in entity_queries[entity]:
        print(f"Trying {entity}, {query}...")
        entity_queries[entity][query] = entity_queries[entity][query].pipe(
            add_threat_intel,
            correlations=[get_ti_pubs],
            tenant_id=TENANT,
            region=REGION,
        )
```

### display_facets

This function displays the results using Panel cards to help organize, sort and filter results.  `additional_columns` parameter can be used to customize output, usually provided by `add_threat_intel`.

```python
display_facets(entity_queries, additional_columns=["tips.found"])
```
