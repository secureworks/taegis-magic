# Taegis Magic

## Tenant Profiles

### Network Range Templates

Taegis Magic can assist with bulk Network Range operations (export, create/modify) using Microsoft Excel templates.

If you need a basic template to add multiple network ranges to [Taegis Tenant Profiles](https://docs.ctpx.secureworks.com/account/tenant_profile/#network-ranges).  This will create a file called `taegis_network_range_template.xlsx` unless otherwise specified.

```bash
taegis tenant-profiles network template generate [--name filename]
```

Network ranges can be exported from Taegis.  This will create a file called `taegis_network_range_export.xlsx` unless otherwise specified:

```bash
taegis tenant-profiles network template export [--name filename] [--tenant tenant_id] [--region taegis_region]
```

Network ranges can be created or modified (by the CIDR).  This will read `taegis_network_range_template.xlsx` unless otherwise specified.

```bash
taegis tenant-profiles network template upload [--name filename] [--tenant tenant_id] [--region taegis_region]
```

#### Network Range fields

`cidr`: This is a [Classless Inter-Domain Routing](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) network range in the form `x.x.x.x/x`.

`description`: This is your description of what the network range represents to your organization.

`is_critical`: This signifies to Taegis whether this network range is of critical importance to your organization.  This field can be set to the following: `'True`, `TRUE`, `=TRUE()`, or `'False`, `FALSE`, `=FALSE()` or any other boolean value supported by Microsoft Excel.

`network_type`: This is the type of network this range is used for.  This can be set to `Internal`, `Public`, `VPN`, `DMZ`, `Guest`, `NAT`, or `Other`.

**Note**: Other fields found in the export document will be ignored when used with `upload`.
