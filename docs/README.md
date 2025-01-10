# Taegis Magic

## Getting Started

### Installation

```bash
python -m pip install taegis-magic
```

### First Command

```bash
taegis users current-user
```

Running the above command displays the user information for the logged-in user.  Logging in is the same as the `Taegis SDK for Python`; OAuth2 with a `CLIENT_ID` and `CLIENT_SECRET`, username/password/mfa, or single-sign on.

```json
[{"id": "xxxxx", "id_uuid": null, "user_id": "auth0|xxxxx", "user_id_v1": "auth0|xxxxx", "created_at": "0000-00-00T00:00:00.000Z", "updated_at": "0000-00-00T00:00:00.000Z","...": "..."}]
```

**Note**: output is truncated for readability

### Signing In

#### OAuth 2

```bash
$ export CLIENT_ID='<client_id>'
$ export CLIENT_SECRET='<client_secret>'
$ taegis subjects current-subject --assign me --display me
```

#### Username/Password

```bash
$ taegis users current-user
Username: user@secureworks.com
Password: 
MFA Token: 12345
```

#### Single Sign-On

```bash
$ taegis users current-user
Username: user@secureworks.com
Copy URL into a browser: https://api.ctpx.secureworks.com/auth/device/code/activate?user_code=XXXX-XXXX
```

This link will bring you to the SSO provider page setup by your organization.

### Help

```bash
$ taegis --help
                                                                                                   
 Usage: taegis [OPTIONS] COMMAND [ARGS]...                                                         
                                                                                                   
 Taegis Magic main callback.                                                                       
                                                                                                   
╭─ Options ───────────────────────────────────────────────────────────────────────────────────────╮
│ --warning                 --no-warning          [default: warning]                              │
│ --verbose                 --no-verbose          [default: no-verbose]                           │
│ --debug                   --no-debug            [default: no-debug]                             │
│ --custom                  --no-custom           [default: no-custom]                            │
│ --sdk-warning             --no-sdk-warning      [default: sdk-warning]                          │
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

The options section **must** be used after `taegis`, but before any commands.

Example (turn off SDK level warnings):

```bash
$ taegis --no-sdk-warning alerts search ...
```

### Configuration

The Taegis Magic provide a number of configurable options under the `taegis configure` command.

#### Regions

If you need a custom region added to `taegis`, use the `taegis configure regions` commands.

* `add [name] [url]` 
* `remove [name]`
* `list`

#### Queries

For assisting with creating investigations within Taegis, you can configure the tool to track `taegis alert search` and `taegis event search` commands.  This only configures the default value for the command, which can still be turned on or off at run time.

`taegis configure queries track --status yes`

To turn off query tracking:

`taegis configure queries track --status no`

To see the configured option:

`taegis configure queries list`

This also changes the default value for the command:

```bash
$ taegis configure queries list
[{"name": "track", "value": "no"}]
$ taegis alerts search --help
--track    --no-track    [default: no-track]
```

```bash
$ taegis configure queries list
[{"name": "track", "value": "yes"}]
$ taegis alerts search --help
--track    --no-track    [default: track]
```

**Note**: output has been truncated for readability.

#### Logging

Taegis Magic is built on top of the Taegis SDK for Python.  There are two logging modules that we can configure to increase or descrease the verbosity of the logs: `taegis_magic` and `taegis_sdk_python`.  The SDK-specific logging options are prepended with `sdk_`, the magic logging options are not.

* `trace` (for tracing function calls with inputs/outputs; extremely verbose) 
* `debug` (turns on debug log messages)
* `verbose` (turns on informational logging)
* `warning` (turns on warning logging)

By default, Taegis Magic sets the log level to warning and above.  Configuration sets the logging level to the lowest level configured (e.g., setting both warning and debug to `true`  results in debug logging)

```bash
$ taegis configure logging defaults [option] --status [true/false]
```

### Formatting

For CLI formatting, the output is designed for use with JSON parsing tools like `jq`.  The magic do not provide any built in methods for configuring or handling output.

Be sure to be authenticated to Taegis before using a formatting tool, as the login prompts may not display.

### Usage

As the primary purpose of this library is for usage in Jupyter Notebooks, most of the specific examples will be under the Jupyter section, but general examples for how to translate to CLI will be provided.

* Find CLI usage examples: [here](cli/README.md)
* Find Jupyter Notebook usage examples [here](jupyter/README.md)
