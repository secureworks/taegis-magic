# Taegis Magic

Taegis Magic is a Jupyter Notebook and Command Line Interface for interacting with the [Secureworks](https://www.secureworks.com/) [Taegis](https://www.secureworks.com/products/taegis)™ security platform.  The Magics project is intended to assist users with workflows and analysis through [Jupyter Notebook](https://jupyter.org/) integrations and [Pandas](https://pandas.pydata.org/) [DataFrames](https://pandas.pydata.org/docs/reference/frame.html).

## Installation

```bash
python -m pip install taegis-magic
```

## Help

```bash
$ taegis --help

 Usage: taegis [OPTIONS] COMMAND [ARGS]...                                                         
                                                                                                   
 Taegis Magic main callback.                                                                       
                                                                                                   
╭─ Options ───────────────────────────────────────────────────────────────────────────────────────╮
│ --warning                 --no-warning          [default: warning]                              │
│ --verbose                 --no-verbose          [default: no-verbose]                           │
│ --debug                   --no-debug            [default: no-debug]                             │
│ --trace                   --no-trace            [default: no-trace]                             │
│ --sdk-warning             --no-sdk-warning      [default: no-sdk-warning]                       │
│ --sdk-verbose             --no-sdk-verbose      [default: no-sdk-verbose]                       │
│ --sdk-debug               --no-sdk-debug        [default: no-sdk-debug]                         │
│ --install-completion                            Install completion for the current shell.       │
│ --show-completion                               Show completion for the current shell, to copy  │
│                                                 it or customize the installation.               │
│ --help                -h                        Show this message and exit.                     │
╰─────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ──────────────────────────────────────────────────────────────────────────────────────╮
│ alerts                                                                                          │
│ audits                                                                                          │
│ clients                                                                                         │
│ configure                                                                                       │
│ events                                                                                          │
│ investigations                                                                                  │
│ preferences                                                                                     │
│ rules                                                                                           │
│ tenants                                                                                         │
│ threat                                                                                          │
│ users                                                                                           │
╰─────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Sample Usage

For more in depth examples see [docs](https://github.com/secureworks/taegis-magic/blob/main/docs/README.md).

### CLI

```bash
taegis alerts search --limit 2 --cell "FROM alert EARLIEST=-1d" --graphql-output "alerts { list { id metadata { title } } }"
```

### IPython Magic

```python
%load_ext taegis_magic
```

```python
%%taegis alerts search --limit 10 --graphql-output "alerts { list { id metadata { title } } }" --assign df --display df
FROM alert
EARLIEST=-1d
```

|    | id                                                                                 | metadata.title            |
|---:|:-----------------------------------------------------------------------------------|:--------------------------|
|  0 | alert://priv:event-filter:xxxxx:1668534654520:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
|  1 | alert://priv:event-filter:xxxxx:1668534458035:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
|  2 | alert://priv:event-filter:xxxxx:1668534458036:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
|  3 | alert://priv:event-filter:xxxxx:1668534458037:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
|  4 | alert://priv:event-filter:xxxxx:1668534458038:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
|  5 | alert://priv:event-filter:xxxxx:1668534458039:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
|  6 | alert://priv:event-filter:xxxxx:1668534458040:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
|  7 | alert://priv:event-filter:xxxxx:1668534458040:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
|  8 | alert://priv:event-filter:xxxxx:1668534458040:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
|  9 | alert://priv:event-filter:xxxxx:1668534458042:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx | AWS - GetCredentialReport |
