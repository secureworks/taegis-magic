# Taegis Magic

## Notebooks

### Create

Taegis Magic provides a template that provides much of the boiler plate to create an investigation automation using the `taegis notebook create [output_notebook]` command.  This command will place a new template at the specified output notebook path.  This template provides a parameterized notebook to start with Taegis data in a Jupyter Notebook, starter alert/event searches, sample data transformations, data filters, evidence staging, transforming the notebook to a markdown report, and how to create an investigation from the notebook.

### Execute

Taegis Magic provides a Jupyter Notebook executor powered by [papermill](https://papermill.readthedocs.io/en/latest/), but with additional features to ease integrating with the Taegis platform.  Playbook execution will request your Taegis user credentials (Password/MFA or SSO) if a CLIENT_ID/CLIENT_SECRET are not set or you do not have a valid access token in `~/.taegis_sdk_python/config`.

#### Example

```bash
taegis notebook create test.ipynb
taegis notebook execute test.ipynb --region [US1|US2|US3|EU]
```

```bash
$ ls -lh
-rw-r--r-- 1 user group   8845 Jul 16 18:58 test.ipynb
-rw-r--r-- 1 user group  30533 Jul 16 18:59 test_US1.ipynb
-rw-r--r-- 1 user group   1553 Jul 16 18:59 test_US1.report.md
```

> Note: Partners/Organizations
>
> This workflow will not work on a parent tenant due to the event search.  Notebooks that only search alerts will work from a partner or organization level.

The same template may be used against a list of Taegis Tenant IDs.  Tenant data will be stored per generated notebook and reports will be made per tenant.

```bash
tenants = ("xxxxx yyyyy zzzzz")
for tenant in $tenants
do 
    taegis notebook execute test.ipynb --tenant $tenant --region US1
done
```

```bash
$ ls -l
-rw-r--r-- 1 user group   8845 Jul 16 18:58 test.ipynb
-rw-r--r-- 1 user group  30533 Jul 16 18:59 test_US1_xxxxx.ipynb
-rw-r--r-- 1 user group   1553 Jul 16 18:59 test_US1_xxxxx.report.md
-rw-r--r-- 1 user group  39864 Jul 16 19:12 test_US1_yyyyy.ipynb
-rw-r--r-- 1 user group   1893 Jul 16 19:13 test_US1_yyyyy.report.md
-rw-r--r-- 1 user group 150962 Jul 16 19:25 test_US1_yyyyy.ipynb
-rw-r--r-- 1 user group   2458 Jul 16 19:27 test_US1_zzzzz.report.md
```

### Generate Report

Taegis Magic also supplies a means of turning a Jupyter Notebook into a markdown report for use with Investigations Keyfindings.  This functionality is also offered in the notebook itself via the `%generate_report` magic.

```bash
taegis notebook generate-report test_US1.ipynb
```

Cells may be marked with `remove_cell` tag to have the code/markdown cell be removed from the report, but included for user reference in the notebook.
