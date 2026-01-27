"""Magics Configuration tool."""

import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import List

import typer
from dataclasses_json import dataclass_json
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from typing_extensions import Annotated

from taegis_sdk_python.config import get_config, write_config, write_to_config
from taegis_sdk_python.middlewares.retry._default import SECTION as RETRY_SECTION

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Magic Configuration Commands.")

auth = typer.Typer(help="Configure Authentication options.")
regions = typer.Typer(help="Configure Addtional Regions Commands.")
queries = typer.Typer(help="Configure Default Query Commands.")
configure_logging = typer.Typer(help="Configure Magic Logging Commands.")
middlewares = typer.Typer(help="Configure HTTP Middleware modules.")
middlewares_retry = typer.Typer(help="Configure HTTP Retry middleware.")

template = typer.Typer(help="Configure Template options.")

middlewares.add_typer(middlewares_retry, name="retry")

app.add_typer(auth, name="auth")
app.add_typer(middlewares, name="middlewares")
app.add_typer(regions, name="regions")
app.add_typer(template, name="template")
app.add_typer(queries, name="queries")
app.add_typer(configure_logging, name="logging")


AUTH_SECTION = "magics.auth"
REGIONS_SECTION = "magics.regions"
QUERIES_SECTION = "magics.queries"
LOGGING_SECTION = "magics.logging"
MIDDLEWARES_SECTION = "magics.middlewares"


class UseUniversalAuthOptions(str, Enum):
    """Configure default logging options."""

    true = "true"
    false = "false"


class LoggingOptions(str, Enum):
    """Configure Taegis Magic logging."""

    trace = "trace"
    debug = "debug"
    verbose = "verbose"
    warning = "warning"
    sdk_debug = "sdk_debug"
    sdk_verbose = "sdk_verbose"
    sdk_warning = "sdk_warning"


class ConfigureLogging(str, Enum):
    """Configure default logging options."""

    true = "true"
    false = "false"


class QueriesTrack(str, Enum):
    """Automatically track alert/event search queries."""

    yes = "yes"
    no = "no"


class MiddleswareAvailable(str, Enum):
    """AIOHTTP Middlewares available."""

    retry = "retry"
    logging = "logging"


class DisableReturnDisplay(str, Enum):

    off = "off"
    all = "all"
    on_empty = "on_empty"


@dataclass_json
@dataclass
class ConfigurationNormalizer(TaegisResultsNormalizer):
    """Configuration Normalizer."""


@tracing
def set_defaults():  # pragma: no cover
    """Set default configuration values for Taegic Magics."""
    config = get_config()

    ###
    # Add Sections
    ###
    if not config.has_section(AUTH_SECTION):
        config.add_section(AUTH_SECTION)

    if not config.has_section(REGIONS_SECTION):
        config.add_section(REGIONS_SECTION)

    if not config.has_section(QUERIES_SECTION):
        config.add_section(QUERIES_SECTION)

    if not config.has_section(LOGGING_SECTION):
        config.add_section(LOGGING_SECTION)

    if not config.has_section(MIDDLEWARES_SECTION):
        config.add_section(MIDDLEWARES_SECTION)

    if not config.has_section(RETRY_SECTION):
        config.add_section(RETRY_SECTION)

    ###
    # Auth Defaults
    ###
    if not config.has_option(AUTH_SECTION, "use_universal_auth"):
        config[AUTH_SECTION]["use_universal_auth"] = "false"

    ###
    # Queries Defaults
    ###
    if not config.has_option(QUERIES_SECTION, "track"):
        config[QUERIES_SECTION]["track"] = "no"

    ###
    # Logging Defaults
    ###
    if not config.has_option(LOGGING_SECTION, LoggingOptions.warning):
        config[LOGGING_SECTION][LoggingOptions.warning.value] = "true"
    if not config.has_option(LOGGING_SECTION, LoggingOptions.verbose):
        config[LOGGING_SECTION][LoggingOptions.verbose.value] = "false"
    if not config.has_option(LOGGING_SECTION, LoggingOptions.debug):
        config[LOGGING_SECTION][LoggingOptions.debug.value] = "false"
    if not config.has_option(LOGGING_SECTION, LoggingOptions.trace):
        config[LOGGING_SECTION][LoggingOptions.trace.value] = "false"
    if not config.has_option(LOGGING_SECTION, LoggingOptions.sdk_warning):
        config[LOGGING_SECTION][LoggingOptions.sdk_warning.value] = "true"
    if not config.has_option(LOGGING_SECTION, LoggingOptions.sdk_verbose):
        config[LOGGING_SECTION][LoggingOptions.sdk_verbose.value] = "false"
    if not config.has_option(LOGGING_SECTION, LoggingOptions.sdk_debug):
        config[LOGGING_SECTION][LoggingOptions.sdk_debug.value] = "false"

    ###
    # Middleware Defaults
    ###
    if not config.has_option(MIDDLEWARES_SECTION, MiddleswareAvailable.retry):
        config[MIDDLEWARES_SECTION][MiddleswareAvailable.retry.value] = "false"
    if not config.has_option(MIDDLEWARES_SECTION, MiddleswareAvailable.logging):
        config[MIDDLEWARES_SECTION][MiddleswareAvailable.logging.value] = "false"

    if not config.has_option(RETRY_SECTION, "max_time"):
        config[RETRY_SECTION]["max_time"] = "10"
    if not config.has_option(RETRY_SECTION, "max_tries"):
        config[RETRY_SECTION]["max_tries"] = "0"

    write_config(config)

    return config


@auth.command("list")
@tracing
def auth_list():
    """Configure use of Universal Authentication."""
    config = get_config()

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[
            {
                name: value,
            }
            for name, value in config[AUTH_SECTION].items()
        ],
    )
    return results


@auth.command("use-universal-auth")
@tracing
def use_universal_auth(
    value: Annotated[
        UseUniversalAuthOptions,
        typer.Argument(help="Use Universal Authentication for Taegis Magic."),
    ] = UseUniversalAuthOptions.false,
):
    """Configure use of Universal Authentication."""
    config = get_config()

    config[AUTH_SECTION]["use_universal_authentication"] = value.value

    write_config(config)

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(use_universal_authentication=value.value)],
    )
    return results


@regions.command(name="add")
@tracing
def add_regions(
    name: str,
    url: str,
):
    """Configure addition Taegis regions for magics."""
    write_to_config(
        REGIONS_SECTION,
        name,
        url,
    )

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(name=name, url=url)],
    )
    return results


@regions.command(name="remove")
@tracing
def remove_regions(name: str):
    """Remove additional Taegis regions from configuration."""
    config = get_config()

    config.remove_option(REGIONS_SECTION, name)

    write_config(config)

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(name=name)],
    )
    return results


@regions.command(name="list")
@tracing
def list_regions():
    """List additional regions."""
    config = get_config()

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[
            dict(
                name=name,
                url=url,
            )
            for name, url in config[REGIONS_SECTION].items()
        ],
    )
    return results


@queries.command(name="track")
@tracing
def queries_track(status: QueriesTrack = QueriesTrack.no):
    """Configure Alert/Event query tracking by default."""
    write_to_config(
        QUERIES_SECTION,
        "track",
        status.value,
    )

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(status=status.value)],
    )
    return results


@queries.command(name="list")
@tracing
def queries_list():
    """List queries configurations."""
    config = get_config()

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[
            dict(
                name=name,
                value=value,
            )
            for name, value in config[QUERIES_SECTION].items()
        ],
    )
    return results


@queries.command(name="callername")
@tracing
def callername(
    name: Annotated[
        str,
        typer.Argument(
            help="Caller Name to use for Alert/Event queries; used for searching/filtering results from the Queries API."
        ),
    ],
):
    """Configure Alert/Event query callername by default."""
    write_to_config(
        QUERIES_SECTION,
        "callername",
        name,
    )

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(callername=name)],
    )
    return results


@queries.command(name="disable-return-display")
@tracing
def queries_disable_return_display(
    status: Annotated[
        DisableReturnDisplay, typer.Argument()
    ] = DisableReturnDisplay.off,
):
    """Configure query disable-return-display default."""
    write_to_config(
        QUERIES_SECTION,
        "disable-return-display",
        status.value,
    )

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(status=status.value)],
    )
    return results


@configure_logging.command(name="defaults")
@tracing
def logging_defaults(
    option: LoggingOptions, status: Annotated[ConfigureLogging, typer.Option()]
):
    """Configure logging defaults."""
    write_to_config(
        LOGGING_SECTION,
        option.value,
        status.value,
    )

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(option=option.value, status=status.value)],
    )
    return results


@template.command(name="path")
@tracing
def template_path(
    path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            dir_okay=True,
            writable=False,
            readable=True,
            resolve_path=True,
        ),
    ],
):
    """Configure the templates path for cell templates."""

    write_to_config("templates", "jinja2", str(path))

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(path=str(path))],
    )
    return results


@middlewares.command("toggle")
@tracing
def middlewares_toggle(
    middleware: Annotated[
        MiddleswareAvailable, typer.Argument(help="Middleware to toggle.")
    ],
    on: Annotated[
        bool, typer.Option("--on/--off", help="Toggle middleware on/off.")
    ] = False,
):
    """Configure toggle for middlewares."""
    write_to_config(
        MIDDLEWARES_SECTION,
        middleware.value,
        str(on),
    )

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[{middleware: on}],
    )
    return results


@middlewares.command("list")
@tracing
def middlewares_list():
    """List middleware toggles."""
    config = get_config()

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[
            dict(
                name=name,
                value=value,
            )
            for name, value in config[MIDDLEWARES_SECTION].items()
        ],
    )
    return results


@middlewares_retry.command("max_tries")
@tracing
def middlewares_retry_max_tries(
    max_tries: Annotated[
        int, typer.Argument(help="Max calls to retry.  0 for unlimited.", min=0)
    ] = 0,
):
    """Configure retry middleware max_tries."""
    write_to_config(
        RETRY_SECTION,
        "max_tries",
        str(max_tries),
    )

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(max_tries=max_tries)],
    )
    return results


@middlewares_retry.command("max_time")
@tracing
def middlewares_retry_max_tries(
    max_time: Annotated[
        int,
        typer.Argument(
            help="Max time in seconds to retry. 0 for no retry time.", min=0
        ),
    ] = 10,
):
    """Configure retry middleware time."""
    write_to_config(
        RETRY_SECTION,
        "max_time",
        str(max_time),
    )

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(max_time=max_time)],
    )
    return results


@middlewares_retry.command("list")
@tracing
def middlewares_retry_list():
    """List middleware toggles."""
    config = get_config()

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[
            dict(
                name=name,
                value=value,
            )
            for name, value in config[RETRY_SECTION].items()
        ],
    )
    return results
