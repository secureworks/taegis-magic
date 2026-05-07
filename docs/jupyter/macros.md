# Taegis Magic

## Tenant Macros

**Note**: Each block represents a Jupyter Notebook cell.

Tenant Macros let you reference a dynamic group of tenants with a short
`@name` shorthand instead of hard-coding a list of tenant IDs.  When a
macro is supplied to the `--tenant` flag, Taegis Magic resolves the macro
to a list of tenant IDs via the `tenantsv4` API and runs the command
once per tenant, merging the results into a single DataFrame.

### Quick Example

```
%%taegis alerts search --tenant @mdr --assign alerts
FROM alert
WHERE severity >= 0.6
EARLIEST=-1d
```

The `@mdr` macro resolves at runtime to every tenant subscribed to one
of the MDR services and the search runs across all of them.  The
resulting DataFrame includes a `_macro_tenant_id` column identifying
which tenant each record came from.

### Default Macros

Taegis Magic ships with a default `@mdr` macro that matches tenants
subscribed to any MDR-style service (ManagedXDR Essentials, MDR, Dell
SafeGuard MDR, MXDR POC, EIR Monitoring, and iSensor Only).  This macro
works out of the box with no configuration.

### Listing Available Macros

To see the macros available in your current configuration along with
the resolved resource path:

```bash
taegis configure macros list
```

### Customising the Macros File

You can point Taegis Magic at your own YAML file to define additional
macros or override the defaults.  Provide the path to a readable YAML
file using `taegis configure macros path`:

```bash
taegis configure macros path /path/to/my_macros.yaml
```

To revert back to the bundled default macros:

```bash
taegis configure macros reset
```

### Custom Macros YAML Format

The YAML file uses a top-level `macros:` key with one entry per macro.
Each macro entry supports filters that map onto fields of the
`tenantsv4` `TenantsQuery` input type.

```yaml
macros:
  mdr:
    services:
      - "*MDR*"
      - "*ManagedXDR Essentials*"
      - "*Dell SafeGuard*"
```

Supported fields per macro:

| Field           | Description                                                     |
| --------------- | --------------------------------------------------------------- |
| `services`      | Tenant subscription/service names (supports `*` wildcard)       |


Multiple values within a single field are combined with OR semantics
(a tenant matches the macro if it satisfies any of the values).

### Multi-Tenant Result Merging

When a macro resolves to more than one tenant, the command is executed
once per tenant and the individual result sets are merged into a single
DataFrame.

For commands that resolve to a single tenant (or are invoked with a
plain tenant ID), behaviour is unchanged and no extra column is added.

### Passing a Plain Tenant ID

The macro syntax is purely additive — supplying a tenant ID directly to
`--tenant` continues to work exactly as before:

```
%%taegis alerts search --tenant 11063 --assign alerts
FROM alert
EARLIEST=-1d
```

Only values that begin with `@` are treated as macro references.
