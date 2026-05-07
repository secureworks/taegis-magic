# Taegis Magic

This notebook demonstrates the usage of Taegis Magic search commands and pandas functions for interacting with Taegis Search features.

## Search

### Generate Taegis QL from Natural Language

Use the `generate` command to convert a natural language query into Taegis QL.

```python
%%taegis search generate 
Find all alerts related to malware in the last 24 hours
```

### List Previous Queries

List all previous natural language search queries stored in the database.

```python
%taegis search list
```

### Delete a Query

Remove a specific search query by its ID.

```python
%taegis search delete --query-id "some-uuid"
```

### Clear All Queries

Remove all stored search queries.

```python
%taegis search clear
```

### Provide Feedback

Give feedback on a generated query, including rating, comments, and suggestions.

```python
%taegis search feedback THUMBS_UP --query-id "@last" --comment "This query worked well" --suggestion "Add more filters"
```

### List Prompt Categories

Retrieve example prompt categories for Taegis Search.

```python
%taegis search prompt-categories --assign categories
```

### Inflate Prompt Examples

The `inflate_prompt_examples` function takes a DataFrame from the `prompts` command and explodes nested prompt data into individual rows with normalized columns.

```python
from taegis_magic.pandas.search import inflate_prompt_examples

inflated_df = categories.pipe(inflate_prompt_examples)
```

### List Example Prompts

Retrieve example prompts for Taegis Search.

```python
%taegis search prompts
```
