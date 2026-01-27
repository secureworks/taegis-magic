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

Running the above command displays the user information for the logged-in user.

```json
[{"id": "xxxxx", "id_uuid": null, "user_id": "auth0|xxxxx", "user_id_v1": "auth0|xxxxx", "created_at": "0000-00-00T00:00:00.000Z", "updated_at": "0000-00-00T00:00:00.000Z","...": "..."}]
```

**Note**: output is truncated for readability

### Signing In

Taegis authentication is handled by the Taegis SDK for Python.  Automations may be configured by setting the CLIENT_ID and CLIENT_SECRET environment variables.  Environment variable references may be customized by the Taegis SDK for Python.  If the CLIENT_ID and CLIENT_SECRET are not present, users will be requested to submit their Taegis User Email and a Device Code Authentication link back into the Taegis Portal.

Authentication is handled at command runtime.  Access tokens are cached in `~/.taegis_sdk_python/config`.  Explicit authentication before a command is not required.  `taegis auth login` is available to authenticate as it's own command.  Access tokens may be explicitly removed from the cache with `taegis auth logout`.

#### OAuth2

```bash
$ export CLIENT_ID='<client_id>'
$ export CLIENT_SECRET='<client_secret>'
$ taegis auth login
```

#### User

All user sign-ins are through Device Code Authentication.  A URL will be presented to the user where the Taegis Portal will determine if their organization is setup for Single Sign On or Taegis Password and MFA grants.  Sign-ins will timeout after 5 minutes.

```bash
$ taegis auth login
Copy URL into a browser: https://api.ctpx.secureworks.com/auth/device/code/activate?user_code=XXXX-XXXX
```

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

The options section **must** be used after `taegis`, but before any subcommands.

Example (turn off SDK level warnings):

```bash
$ taegis --no-sdk-warning alerts search ...
```

### Configuration

The Taegis Magic provide a number of configurable options under the `taegis configure` command.

#### Authentication

Taegis Magic supports a unified endpoint for authentication.  The subject (User or Client) will need to be enabled in multiple regions as well.  The default behavior is to authenticate per region.  Setting to true will enable a single `access_token` from a `universal` region.

```bash
taegis configure auth use-universal-auth true
```

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

The magic output results metadata table cay be configured to be turned off for empty results or all results.  Setting to off will display the metadata table when `--display` is not used.

```
**Taegis Results**

|Region          |Tenant             |Service          |Total Results                       |
|----------------|-------------------|-----------------|------------------------------------|
|charlie|None|users|1|
```

```bash
$ taegis configure queries disable-return-display on_empty
[{"status": "on_empty"}]
$ taegis configure queries disable-return-display all
[{"status": "all"}]
$ taegis configure queries disable-return-display off
[{"status": "all"}]
$ taegis configure queries list
[{"status": "on_empty"}]
```

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

For CLI formatting, the output is designed for use with JSON parsing tools like `jq`.  The magic does not provide any built in methods for configuring or handling output.

Be sure to be authenticated to Taegis before using a formatting tool, as the login prompts may not display.

### Usage

As the primary purpose of this library is for usage in Jupyter Notebooks, most of the specific examples will be under the Jupyter section, but general examples for how to translate to CLI will be provided.

* Find CLI usage examples: [here](cli/README.md)
* Find Jupyter Notebook usage examples [here](jupyter/README.md)
