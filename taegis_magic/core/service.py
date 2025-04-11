"""Taegis Magic Service Generator."""

from taegis_magic._version import __version__
from taegis_magic.commands.configure import (
    AUTH_SECTION,
    REGIONS_SECTION,
    use_universal_auth,
)

from taegis_sdk_python import GraphQLService
from taegis_sdk_python._consts import TAEGIS_ENVIRONMENT_URLS
from taegis_sdk_python.config import get_config


def get_service(*args, **kwargs) -> GraphQLService:
    """Get a configured Taegis GraphQL Service object."""
    config = get_config()

    if not config.has_section(AUTH_SECTION):
        config.add_section(AUTH_SECTION)

    if not config.has_section(REGIONS_SECTION):
        config.add_section(REGIONS_SECTION)

    regions = config[REGIONS_SECTION]
    additional_regions = {k: v for k, v in regions.items() if k and v}

    environments = {**TAEGIS_ENVIRONMENT_URLS, **additional_regions}

    if (
        use_universal_authentication := kwargs.get("use_universal_authentication")
    ) is None:
        use_universal_authentication = config[AUTH_SECTION].getboolean(
            "use_universal_authentication", False
        )

    kwargs.pop("use_universal_authentication", None)

    if "environments" in kwargs:
        environments.update(kwargs["environments"])
        kwargs.pop("environments", None)

    extra_headers = {
        "User-Agent": f"taegis_magic/{__version__}",
        "apollographql-client-name": "taegis_magic",
        "apollographql-client-version": __version__,
    }

    if "extra_headers" in kwargs:
        extra_headers.update(kwargs["extra_headers"])
        kwargs.pop("extra_headers", None)

    return GraphQLService(
        environments=environments,
        extra_headers=extra_headers,
        use_universal_authentication=use_universal_authentication,
        *args,
        **kwargs,
    )
