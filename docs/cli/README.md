# Taegis Magic

## Command Line Interface (CLI)

While the Taegis Magic are intended to support use in Jupyter Notebooks, the tool is also designed to work as a command line interface (CLI) as well.  The same results that are placed into the Pandas DataFrame in notebooks are displayed as JSON (JavaScript Object Notation).

## Cell Magic Handling

In order for the cell magic to work, the cell portion is passed into the command with the `--cell` option.  This is a generic option for passing in the arbitrary text block but is available in the CLI tool.

## Differences

When a command wants a reference to a Pandas DataFrame (i.e., `taegis investigations evidence stage`), Taegis Magic can utilize a JSON file with the same shape.  For functionality like tracking investigation evidence, Taegis Magic can be stored as a local database file instead of in memory.

Example:

```bash
$ taegis alerts search --cell "FROM alert WHERE status = 'OPEN' AND metadata.severity >= 0.2 AND investigation_ids IS NULL AND metadata.title = 'Suspicious AWS Account Enumeration'| head 2" --track --database test_database.db > test_results.json
$ taegis events search --cell "FROM cloudaudit WHERE user_name CONTAINS 'jupiter' EARLIEST=-3d | head 2" --track --database test_database.db > test_events.json
$ taegis investigations evidence stage alerts test_results.json --database test_database.db
$ taegis investigations evidence stage events test_events.json --database test_database.db
$ taegis investigations evidence stage search_queries --database test_database.db
$ taegis investigations evidence show --database test_database.db
$ echo "This is a a test" > test_kf.md
$ taegis investigations create --title "CLI Test" --key-findings test_kf.md --priority LOW --type SECURITY_INVESTIGATION --status OPEN --assignee-id @customer --database test_database.db
```

## Further Reading

* [Tenant Profiles](tenant-profiles.md)
