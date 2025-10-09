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
