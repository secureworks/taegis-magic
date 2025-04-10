"""Magics Configuration tool."""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import List

import typer
from dataclasses_json import dataclass_json
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from typing_extensions import Annotated

from taegis_sdk_python.config import get_config, write_config, write_to_config

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Magic Configuration Commands.")

auth = typer.Typer(help="Configure Authentication options.")
regions = typer.Typer(help="Configure Addtional Regions Commands.")
queries = typer.Typer(help="Configure Default Query Commands.")
configure_logging = typer.Typer(help="Configure Magic Logging Commands.")

app.add_typer(auth, name="auth")
app.add_typer(regions, name="regions")
app.add_typer(queries, name="queries")
app.add_typer(configure_logging, name="logging")

AUTH_SECTION = "magics.auth"
REGIONS_SECTION = "magics.regions"
QUERIES_SECTION = "magics.queries"
LOGGING_SECTION = "magics.logging"


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
    ] = UseUniversalAuthOptions.false
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
    ]
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
