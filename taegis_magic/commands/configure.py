"""Magics Configuration tool."""

from configparser import SectionProxy
import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import typer
from dataclasses_json import dataclass_json
from taegis_magic.core.log import tracing, TRACE_LOG_LEVEL, get_module_logger, get_sdk_logger
from taegis_magic.core.normalizer import TaegisResultsNormalizer, TaegisResultWithMessage
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
macros = typer.Typer(help="Configure Tenant Macro Commands.")

middlewares.add_typer(middlewares_retry, name="retry")

app.add_typer(auth, name="auth")
app.add_typer(macros, name="macros")
app.add_typer(middlewares, name="middlewares")
app.add_typer(regions, name="regions")
app.add_typer(template, name="template")
app.add_typer(queries, name="queries")
app.add_typer(configure_logging, name="logging")


AUTH_SECTION = "magics.auth"
MACROS_SECTION = "magics.macros"
REGIONS_SECTION = "magics.regions"
QUERIES_SECTION = "magics.queries"
LOGGING_SECTION = "magics.loggers"
MIDDLEWARES_SECTION = "magics.middlewares"
MAGIC_LOG_LEVEL_KEY = "magic_log_level"
SDK_LOG_LEVEL_KEY = "sdk_log_level"


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


class MagicLoggerLevel(str, Enum):
    """Allowed taegis_magic logger levels."""

    trace = "trace"
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"


class SdkLoggerLevel(str, Enum):
    """Allowed taegis_sdk_python logger levels."""

    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"


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


def _set_magic_logger_level(level: MagicLoggerLevel):
    taegis_magics_logger = get_module_logger()
    if level == MagicLoggerLevel.trace:
        taegis_magics_logger.setLevel(TRACE_LOG_LEVEL)
    elif level == MagicLoggerLevel.debug:
        taegis_magics_logger.setLevel(logging.DEBUG)
    elif level == MagicLoggerLevel.info:
        taegis_magics_logger.setLevel(logging.INFO)
    elif level == MagicLoggerLevel.warning:
        taegis_magics_logger.setLevel(logging.WARNING)
    else:
        taegis_magics_logger.setLevel(logging.ERROR)


def _set_sdk_logger_level(level: SdkLoggerLevel):
    sdk_logger = get_sdk_logger()
    if level == SdkLoggerLevel.debug:
        sdk_logger.setLevel(logging.DEBUG)
    elif level == SdkLoggerLevel.info:
        sdk_logger.setLevel(logging.INFO)
    elif level == SdkLoggerLevel.warning:
        sdk_logger.setLevel(logging.WARNING)
    else:
        sdk_logger.setLevel(logging.ERROR)


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

    if not config.has_section(MACROS_SECTION):
        config.add_section(MACROS_SECTION)

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
    if not config.has_option(LOGGING_SECTION, MAGIC_LOG_LEVEL_KEY):
        config[LOGGING_SECTION][MAGIC_LOG_LEVEL_KEY] = (
            MagicLoggerLevel.warning.value
        )
    if not config.has_option(LOGGING_SECTION, SDK_LOG_LEVEL_KEY):
        config[LOGGING_SECTION][SDK_LOG_LEVEL_KEY] = (
            SdkLoggerLevel.warning.value
        )

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
    results = TaegisResultWithMessage(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[
            dict(
                deprecated=True,
                message="This command is deprecated and does nothing. Use `configure logging levels` instead.",               
            )
        ],
    )
    return results


@configure_logging.command(name="list")
@tracing
def list_current_config(): 
    """List current logging config"""
    config = get_config()

    return TaegisResultWithMessage(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[
            dict(config[LOGGING_SECTION])
        ],
    )


@configure_logging.command(name="levels")
@tracing
def logging_levels(
    magic_log_level: Annotated[
        MagicLoggerLevel,
        typer.Option(
            help="Set taegis_magic logger level (trace/debug/info/warning/error).",
        ),
    ] = MagicLoggerLevel.warning,
    sdk_log_level: Annotated[
        SdkLoggerLevel,
        typer.Option(
            help="Set taegis_sdk_python logger level (debug/info/warning/error).",
        ),
    ] = SdkLoggerLevel.warning,
):
    """Configure logging levels for magic and SDK loggers."""
    config = get_config()
    if not config.has_section(LOGGING_SECTION):
        config.add_section(LOGGING_SECTION)

    config[LOGGING_SECTION][MAGIC_LOG_LEVEL_KEY] = magic_log_level.value
    config[LOGGING_SECTION][SDK_LOG_LEVEL_KEY] = sdk_log_level.value
    write_config(config)

    _set_magic_logger_level(magic_log_level)
    _set_sdk_logger_level(sdk_log_level)

    results = TaegisResultWithMessage(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[
            dict(config[LOGGING_SECTION])
        ],
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


@macros.command(name="path")
@tracing
def macros_path(
    path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=False,
            readable=True,
            resolve_path=True,
        ),
    ],
):
    """Set a custom YAML resource path for tenant macros."""
    write_to_config(
        MACROS_SECTION,
        "resource_path",
        str(path),
    )

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(resource_path=str(path))],
    )
    return results


@macros.command(name="reset")
@tracing
def macros_reset():
    """Reset tenant macros to the bundled default resource."""
    from taegis_magic.core.macros import DEFAULT_MACROS_RESOURCE

    config = get_config()
    config.remove_option(MACROS_SECTION, "resource_path")
    write_config(config)

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[dict(resource_path=str(DEFAULT_MACROS_RESOURCE), status="reset")],
    )
    return results


@macros.command(name="list")
@tracing
def macros_list():
    """List available tenant macros and the current resource path."""
    from taegis_magic.core.macros import (
        DEFAULT_MACROS_RESOURCE,
        _get_custom_macros_path,
        load_macros,
    )

    custom_path = _get_custom_macros_path()
    source = str(custom_path) if custom_path else str(DEFAULT_MACROS_RESOURCE)
    macro_defs = load_macros(custom_path)

    results = ConfigurationNormalizer(
        service="configure",
        tenant_id="None",
        region="None",
        raw_results=[
            dict(
                resource_path=source,
                macros=[
                    dict(name=name, definition=definition)
                    for name, definition in macro_defs.items()
                ],
            )
        ],
    )
    return results
