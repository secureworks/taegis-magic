"""Taegis Magic CLI definitions."""

import json
import logging
import sys

import typer

from gql.transport.exceptions import TransportQueryError
from taegis_magic.commands import (
    alerts,
    audits,
    authentication,
    clients,
    configure,
    events,
    investigations,
    notebook,
    preferences,
    rules,
    search,
    subjects,
    tenant_profiles,
    tenants,
    threat,
    users,
)
from taegis_magic.core.normalizer import TaegisResult
from dataclasses_json import dataclass_json
from dataclasses import dataclass

from taegis_magic.core.log import TRACE_LOG_LEVEL, get_module_logger

log = logging.getLogger(__name__)


@dataclass_json
@dataclass
class MagicVersion:
    python: str
    taegis_sdk_python: str
    taegis_magic: str


CONTEXT_SETTINGS = dict(help_option_names=["--help", "-h"])

app = typer.Typer(context_settings=CONTEXT_SETTINGS)
app.add_typer(alerts.app, name="alerts")
app.add_typer(authentication.app, name="auth")
app.add_typer(audits.app, name="audits")
app.add_typer(clients.app, name="clients")
app.add_typer(configure.app, name="configure")
app.add_typer(events.app, name="events")
app.add_typer(investigations.app, name="investigations")
app.add_typer(notebook.app, name="notebook")
app.add_typer(preferences.app, name="preferences")
app.add_typer(rules.app, name="rules")
app.add_typer(search.app, name="search")
app.add_typer(subjects.app, name="subjects")
app.add_typer(tenant_profiles.app, name="tenant-profiles")
app.add_typer(tenants.app, name="tenants")
app.add_typer(threat.app, name="threat")
app.add_typer(users.app, name="users")


CONFIG = configure.set_defaults()


@app.callback()
def main(
    warning: bool = CONFIG[configure.LOGGING_SECTION].getboolean(
        "warning", fallback=True
    ),
    verbose: bool = CONFIG[configure.LOGGING_SECTION].getboolean(
        "verbose", fallback=False
    ),
    debug: bool = CONFIG[configure.LOGGING_SECTION].getboolean("debug", fallback=False),
    trace: bool = CONFIG[configure.LOGGING_SECTION].getboolean("trace", fallback=False),
    sdk_warning: bool = CONFIG[configure.LOGGING_SECTION].getboolean(
        "sdk_warning", fallback=True
    ),
    sdk_verbose: bool = CONFIG[configure.LOGGING_SECTION].getboolean(
        "sdk_verbose", fallback=False
    ),
    sdk_debug: bool = CONFIG[configure.LOGGING_SECTION].getboolean(
        "sdk_debug", fallback=False
    ),
):
    """Taegis Magic help menu."""
    logger = get_module_logger()
    if trace:
        logger.setLevel(TRACE_LOG_LEVEL)
    elif debug:
        logger.setLevel(logging.DEBUG)
    elif verbose:
        logger.setLevel(logging.INFO)
    elif warning:
        logger.setLevel(logging.WARNING)
    else:
        logger.setLevel(logging.ERROR)

    sdk_logger = logging.getLogger("taegis_sdk_python")

    if sdk_debug:
        sdk_logger.setLevel(logging.DEBUG)
    elif sdk_verbose:
        sdk_logger.setLevel(logging.INFO)
    elif sdk_warning:
        sdk_logger.setLevel(logging.WARNING)
    else:
        sdk_logger.setLevel(logging.ERROR)


@app.command()
def version():
    """Taegis Magic version information."""
    import sys
    from taegis_sdk_python._version import __version__ as sdk_version
    from taegis_magic._version import __version__ as magic_version

    return TaegisResult(
        raw_results=MagicVersion(
            python=sys.version,
            taegis_sdk_python=sdk_version,
            taegis_magic=magic_version,
        ),
        tenant_id=None,
        region=None,
        service="version",
    )


def cli():
    """
    Run app as a cli command.
    """
    try:
        result = app(standalone_mode=False)
    except TransportQueryError as exc:
        print(json.dumps(exc.errors))
        sys.exit(1)
    except SystemExit:
        sys.exit(0)

    if isinstance(result, int):
        sys.exit(result)

    print(json.dumps(result.results))


if __name__ == "__main__":
    app()
