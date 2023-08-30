# Taegis Magic

## Threat Publications

**Note**: Each block represents a Jupyter Notebook cell.

Find an example notebook with these examples in [Threat](notebooks/Threat.ipynb)


```python
%load_ext taegis_magic

from IPython.display import HTML
```

```python
%taegis threat publications latest 5 --assign pubs --display pubs
```

or 

```python
%taegis threat publications search "GOLD FIESTA" --assign pubs
```

Print a clickable link to Taegis portal:

```python
print(pubs.loc[0, "taegis_magic.reference"])
```

Parse the context:

```python
HTML(pubs.loc[0, "content"])
```

Change the `0` to the index of the context or url your want.
