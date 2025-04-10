import inspect
import logging
from dataclasses import dataclass
from typing import List, Optional

import typer
from taegis_magic.commands.configure import AUTH_SECTION, REGIONS_SECTION
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResult
from taegis_magic.core.service import get_service
from typing_extensions import Annotated

from taegis_sdk_python._consts import TAEGIS_ENVIRONMENT_URLS
from taegis_sdk_python.config import get_config

log = logging.getLogger(__name__)

CONFIG = get_config()
if not CONFIG.has_section(AUTH_SECTION):
    CONFIG.add_section(AUTH_SECTION)

app = typer.Typer(help="Taegis Authentication Commands.")


@dataclass
class AuthenticationResult:
    action: str


@app.command()
@tracing
def login(
    region: Annotated[Optional[List[str]], typer.Option(help="Taegis Region")] = None,
    use_universal_authentication: Annotated[
        bool,
        typer.Option(
            help="Setup authentication to use unified authentication endpoint."
        ),
    ] = CONFIG[AUTH_SECTION].getboolean("use_universal_authentication", fallback=False),
):
    """Log into Taegis environments.  Use '--region all' authenticate each of Taegis environments."""
    service = get_service(use_universal_authentication=use_universal_authentication)

    config = get_config()
    if not config.has_section(REGIONS_SECTION):
        config.add_section(REGIONS_SECTION)

    regions = config[REGIONS_SECTION]
    additional_regions = {k: v for k, v in regions.items() if k and v}

    environments = {**TAEGIS_ENVIRONMENT_URLS, **additional_regions}

    if service.use_universal_authentication:
        print(f"Login for region: {service.authentication_environment}")
        service.access_token
    else:
        if region:
            if "all" in region:
                region = []
                urls = []
                for r, url in environments.items():
                    if url in urls:
                        continue
                    urls.append(url)
                    region.append(r)
            for r in region:
                print(f"Login for region: {environments[r]}")
                with service(environment=r):
                    service.access_token
        else:
            print(f"Login for region: {service.environment}")
            service.access_token

    return TaegisResult(
        raw_results=AuthenticationResult(action="login"),
        service="authentication",
        tenant_id=None,
        region=region,
        arguments=inspect.currentframe().f_locals,
    )


@app.command()
@tracing
def logout(
    region: Annotated[Optional[List[str]], typer.Option(help="Taegis Region")] = None,
    use_universal_authentication: Annotated[
        bool,
        typer.Option(
            help="Setup authentication to use unified authentication endpoint."
        ),
    ] = CONFIG[AUTH_SECTION].getboolean("use_universal_authentication", fallback=False),
):
    """Logout of Taegis environments.  Use '--region all' fully logout of Taegis."""
    service = get_service(use_universal_authentication=use_universal_authentication)

    config = get_config()
    if not config.has_section(REGIONS_SECTION):
        config.add_section(REGIONS_SECTION)

    regions = config[REGIONS_SECTION]
    additional_regions = {k: v for k, v in regions.items() if k and v}

    environments = {**TAEGIS_ENVIRONMENT_URLS, **additional_regions}

    if service.use_universal_authentication:
        print(f"Logout for region: {service.authentication_environment}")
        service.clear_access_token()
    else:
        if region:
            if "all" in region:
                region = []
                urls = []
                for r, url in environments.items():
                    if url in urls:
                        continue
                    urls.append(url)
                    region.append(r)
            for r in region:
                print(f"Logout for region: {environments[r]}")
                with service(environment=r):
                    service.clear_access_token()
        else:
            print(f"Logout for region: {service.environment}")
            service.clear_access_token()

    return TaegisResult(
        raw_results=AuthenticationResult(action="logout"),
        service="authentication",
        tenant_id=None,
        region=region,
        arguments=inspect.currentframe().f_locals,
    )
