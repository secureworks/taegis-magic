import inspect
import logging
from typing import Optional

import typer
from typing_extensions import Annotated

from dataclasses import asdict, dataclass, field
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResult
from taegis_magic.core.service import get_service
from taegis_magic.commands.events import search

from taegis_magic.core.utils import to_dataframe

app = typer.Typer(help="Taegis Subjects Commands.")

log = logging.getLogger(__name__)

from dataclasses import asdict, dataclass
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_sdk_python.services.process_trees.types import ProcessLineage
@dataclass
class ProcessLineageNormalizer(TaegisResultsNormalizer):
    """Normalizer for ProcessLineage results, as it is always a single dataclass result."""

    raw_results: ProcessLineage = field(default_factory=lambda: ProcessLineage())

    @property
    def results(self):
        rows = []
        for result in self.raw_results if isinstance(self.raw_results, list) else [self.raw_results]:
            if hasattr(result, "__dataclass_fields__"):
                result_dict = asdict(result)
            else:
                result_dict = result
            lineage = result_dict.get("lineage", [])
            if lineage is None:
                lineage = []
            for idx, entry in enumerate(lineage):
                row = {"lineage_index": idx}
                if hasattr(entry, "__dataclass_fields__"):
                    row.update(asdict(entry))
                elif isinstance(entry, dict):
                    row.update(entry)
                rows.append(row)
        return rows
    
@app.command()
@tracing
def process_lineage(
    region: Annotated[Optional[str], typer.Option(help="Taegis region")] = None,
    tenant_id: Annotated[Optional[str], typer.Option(help="Taegis tenant id")] = None,
    host_id: Annotated[Optional[str], typer.Option(help="Taegis host id")] = None,
    process_correlation_id: Annotated[Optional[str], typer.Option(help="Taegis Event process correlation id")] = None,
    resource_id: Annotated[Optional[str], typer.Option(help="Taegis Event resource ID")] = None,
):
    """Get process lineage for a given region & tenant, based on the resource_id, host_id, and process_correlation_id."""
    service = get_service(environment=region, tenant_id=tenant_id)

    results = service.process_trees.query.process_lineage(host_id=host_id, process_correlation_id=process_correlation_id, tenant_id=tenant_id, resource_id=resource_id)

    normalized_results = ProcessLineageNormalizer(
        raw_results=results,
        service="process_lineage",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results

@app.command()
@tracing
def process_children(
        region: Annotated[Optional[str], typer.Option(help="Taegis region")] = None,
        tenant_id: Annotated[Optional[str], typer.Option(help="Taegis tenant id")] = None,
        host_id: Annotated[Optional[str], typer.Option(help="Taegis host id")] = None,
        process_correlation_id: Annotated[Optional[str], typer.Option(help="Taegis Event process correlation id")] = None,
        resource_id: Annotated[Optional[str], typer.Option(help="Taegis Event resource ID")] = None,
):
    """Get process children for a given region & tenant, based on the resource_id, host_id, and process_correlation_id."""
    service = get_service(environment=region, tenant_id=tenant_id)
    results = service.process_trees.query.process_children(host_id=host_id, process_correlation_id=process_correlation_id, tenant_id=tenant_id, resource_id=resource_id)

    normalized_results = TaegisResult(
        raw_results=results,
        service="process_children",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command()
@tracing
def process_parent(
        region: Annotated[Optional[str], typer.Option(help="Taegis region")] = None,
        tenant_id: Annotated[Optional[str], typer.Option(help="Taegis tenant id")] = None,
        host_id: Annotated[Optional[str], typer.Option(help="Taegis host id")] = None,
        parent_pcid: Annotated[Optional[str], typer.Option(help="Taegis Event process correlation id")] = None,
        resource_id: Annotated[Optional[str], typer.Option(help="Taegis Event resource ID")] = None,
):
    """Gets the processes' parent based on the parent_correlation_id for a given region & tenant, based on the resource_id, host_id, and parent_process_correlation_id."""
    service = get_service(environment=region, tenant_id=tenant_id)
    results = service.process_trees.query.process_parent(host_id=host_id, parent_process_correlation_id=parent_pcid, tenant_id=tenant_id, resource_id=resource_id)

    normalized_results = TaegisResult(
        raw_results=results,
        service="process_parent",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results

