import inspect
import logging
from dataclasses import dataclass
from typing import List

import typer
from typing_extensions import Annotated

from taegis_magic.commands.configure import REGIONS_SECTION
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResult
from taegis_magic.core.service import get_service
from taegis_sdk_python._consts import TAEGIS_ENVIRONMENT_URLS
from taegis_sdk_python.config import get_config

log = logging.getLogger(__name__)

app = typer.Typer()


@dataclass
class AuthenticationResult:
    action: str


@app.command()
@tracing
def login(
    region: Annotated[List[str], typer.Option()],
):
    """Log into Taegis environments.  Use '--region all' authenticate each of Taegis environments."""
    service = get_service()

    config = get_config()
    if not config.has_section(REGIONS_SECTION):
        config.add_section(REGIONS_SECTION)

    regions = config[REGIONS_SECTION]
    additional_regions = {k: v for k, v in regions.items() if k and v}

    environments = {**TAEGIS_ENVIRONMENT_URLS, **additional_regions}

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
    region: Annotated[List[str], typer.Option()],
):
    """Logout of Taegis environments.  Use '--region all' fully logout of Taegis."""
    service = get_service()

    config = get_config()
    if not config.has_section(REGIONS_SECTION):
        config.add_section(REGIONS_SECTION)

    regions = config[REGIONS_SECTION]
    additional_regions = {k: v for k, v in regions.items() if k and v}

    environments = {**TAEGIS_ENVIRONMENT_URLS, **additional_regions}

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
        service.clear_access_token()

    return TaegisResult(
        raw_results=AuthenticationResult(action="logout"),
        service="authentication",
        tenant_id=None,
        region=region,
        arguments=inspect.currentframe().f_locals,
    )
