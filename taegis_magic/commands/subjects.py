import inspect
import logging
from typing import Optional

import typer
from typing_extensions import Annotated

from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResult
from taegis_magic.core.service import get_service

app = typer.Typer(help="Taegis Subjects Commands.")

log = logging.getLogger(__name__)


@app.command()
@tracing
def current_subject(
    region: Annotated[Optional[str], typer.Option(help="Taegis region")] = None
):
    """Get current subject [TDRUser|Client] information."""
    service = get_service(environment=region)

    results = service.subjects.query.current_subject()

    normalized_results = TaegisResult(
        raw_results=results,
        service="users",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results
