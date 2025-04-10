"""Taegis Magic search commands."""

import logging
from typing import Optional

import typer
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResults
from taegis_magic.core.service import get_service
from taegis_sdk_python.services.nl_search.types import NLSearchInputs
from typing_extensions import Annotated

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Search Commands.")


@app.command()
@tracing
def generate(
    cell: Annotated[
        str, typer.Option(help="Natural language query to convert to Taegis QL")
    ],
    limit: Annotated[int, typer.Option(help="Limit number of results")] = 5,
    region: Annotated[Optional[str], typer.Option(help="Taegis Region")] = None,
):
    """Generate Taegis QL from natural language query."""
    service = get_service(environment=region)

    results = service.nl_search.query.nl_search(
        in_=NLSearchInputs(
            query=cell,
            limit=limit,
        )
    )

    return TaegisResults(
        raw_results=results,
        service="search",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments={
            "cell": cell,
            "limit": limit,
            "region": region,
        },
    )
