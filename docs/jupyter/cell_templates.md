# Taegis Magic

## Cell Templates

Taegis Magic supports Jinja2 templates for cell magic.  The notebook namespace is injected into the template at cell runtime.  This allows for templates to reference objects defined in a Jupyter notebook within templates.

Templates directory location can be configured with the following command:
`taegis configure template path /path/to/templates`

Templates path defaults to the current working directory.

Jinja Template Reference:
https://jinja.palletsprojects.com/en/stable/templates/

Taegis Advanced Search Language Reference:
https://docs.taegis.secureworks.com/search/querylanguage/advanced_search/

## Template Filters

Taegis Magic provides custom Jinja filters for generating Taegis Query Language syntax.  Values are escaped for regular experession and Taegis QL specification.

Generate an OR parameter grouping from a list.  Defaults to '=' operator.
```
{{ list | or(field_name, operator='=') }}
```

Generate an AND parameter grouping from a list.  Defaults to '=' operator.
```
{{ list | and(field_name, operator='=') }}
```

geGenerate an IN parameter grouping from a list.
```
{{ list | in(field_name) }}
```

Generate an !IN parameter grouping from a list.
```
{{ list | not_in(field_name) }}
```

Generate an MATCHES_REGEX parameter grouping from a list.  Defaults to '|' separator.
```
{{ list | regex(field_name, separator='|') }}
```

Generate an MATCHES_REGEX parameter grouping from a list.  Defaults to '|' separator.
```
{{ list | matches_regex(field_name, separator='|') }}
```

Generate an !MATCHES_REGEX parameter grouping from a list.  Defaults to '|' separator.
```
{{ list | not_regex(field_name, separator='|') }}
```

Generate an !MATCHES_REGEX parameter grouping from a list.  Defaults to '|' separator.

```
{{ list | not_matches_regex(field_name, separator='|') }}
```

## Examples

```
# define template variables
ips = ['1.1.1.1', '8.8.8.8']
domains = ['secureworks.com', 'sophos.com']
severity = 0.6
earliest = '-1d'
```

```
%%taegis alerts search --cell-template --assign alerts
FROM alert 
WHERE
    ( 
        {{ ips | in('@ip') }} OR
        {{ domains | regex('@domain') }} 
    ) AND
    severity >= {{ severity }}
EARLIEST={{ earliest }}
```

Example rendered template:
```
FROM alert 
WHERE
    ( 
        @ip IN ('1.1.1.1','8.8.8.8') OR
        @domain MATCHES_REGEX 'secureworks\.com|sophos\.com'
    ) AND
    severity >= 0.6
EARLIEST=-1d
```

> ! Note the following use line magics.

Templates can be defined in a Papermill parameters YAML file.

```yaml
alert_query_template: |
  FROM alert 
  WHERE
      ( 
          {{ ips | in('@ip') }} OR
          {{ domains | regex('@domain') }} 
      ) AND
      severity >= {{ severity }}
  EARLIEST={{ earliest }}
```

```
%taegis alerts search --cell-template --cell "$alert_query_template" --assign alerts
```

Templates can be defined in separate Jinja2 template files.

```
%taegis alerts search --cell-template --cell-template-file "example.ql" --assign alerts
```

## Time Splitting

`alerts search` and `events search` support `--time-window`/`--time-chunk` to break a single query up into a series of smaller, contiguous time ranges that are queried concurrently and aggregated back together into one result set.  This is useful for pulling a large time range of results in event queries, which normally cap out at 30 days.

Both options must be provided together.  Durations are expressed as a number and a unit:

| unit | meaning |
| ---- | ------- |
| `s`  | seconds |
| `m`  | minutes |
| `h`  | hours   |
| `d`  | days    |
| `w`  | weeks   |
| `mo` | months (30d) |
| `y`  | years (365d) |

The following example will produce 5 queries with 4 7d time windows and 1 2d time window (total 30d).  Queries will be run concurrently and aggregated together.

```
%%taegis alerts search --assign alerts_test --time-window 30d --time-chunk 7d
FROM detection | head 5
```

```
%%taegis events search --assign events --time-window 30d --time-chunk 7d
FROM process | head 5
```

### EARLIEST/LATEST handling

Each time chunk needs its own `EARLIEST`/`LATEST` bounds, so Taegis Magic manages them for you.  By default, `EARLIEST='{{ window.earliest }}' LATEST='{{ window.latest }}'` is appended to the query for each chunk.  If the query already sets its own `EARLIEST=`/`LATEST=`, those are stripped out first, since a fixed value would apply to every chunk instead of just its own window and defeat the purpose of chunking.

```
%%taegis alerts search --assign alerts_test --time-window 30d --time-chunk 7d
FROM detection WHERE severity >= 0.6 EARLIEST=-1d LATEST=now
```

is equivalent to:

```
%%taegis alerts search --assign alerts_test --time-window 30d --time-chunk 7d
FROM detection WHERE severity >= 0.6
```

If you need the placeholders somewhere other than the end of the query (for example inside a subquery), include them yourself and they will be used as-is instead of being appended:

```
%%taegis events search --assign events --time-window 30d --time-chunk 7d
FROM process EARLIEST='{{ window.earliest }}' LATEST='{{ window.latest }}' | head 5
```

### Shareable links and tracking

Because each time chunk is a separate query with its own query ID, `shareable_url` returns a newline separated list of links, one per chunk, whenever more than one chunk returned results.  A single link cannot represent every chunk's results.

Similarly, `--track` records one row per chunk's query ID in the search queries database, rather than collapsing them into a single ID.

### Restrictions

- `--time-window` and `--time-chunk` must be set together.
- `--ai` cannot be combined with time chunking.
