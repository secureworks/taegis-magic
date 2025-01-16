"""Taegis Magic tenants commands."""

import inspect
import json
import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Tuple, Optional

import typer
from dataclasses_json import config, dataclass_json
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer

from taegis_magic.core.service import get_service
from taegis_sdk_python.services.tenants.types import (
    OrderDir,
    TenantEnvironmentFilter,
    TenantLabelFilter,
    TenantOrderField,
    TenantResults,
    TenantsQuery,
    TimeFilter,
)
from typing_extensions import Annotated
from click.exceptions import BadOptionUsage

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Tenant Commands.")


@dataclass_json
@dataclass
class TaegisTenantsResultsNormalizer(TaegisResultsNormalizer):
    """Taegis TenantResults normalizer."""

    raw_results: List[TenantResults] = field(default_factory=lambda: [TenantResults()])

    @property
    def results(self):
        return [
            asdict(tenant) for result in self.raw_results for tenant in result.results
        ]

    @property
    def total_results(self) -> int:
        return int(self.raw_results[0].total_count) if self.raw_results else -1

    @property
    def results_returned(self) -> int:
        """Number of results returned from service."""
        return len(self.results)


@app.command()
@tracing
def search(
    filter_by_name: Annotated[
        Optional[str], typer.Option(help="(supports wildcard %) or tenant id")
    ] = None,
    filter_by_tenant: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_region: Annotated[Optional[str], typer.Option()] = None,
    filter_by_service: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_label_name: Annotated[Optional[str], typer.Option()] = None,
    filter_by_label_value: Annotated[
        Optional[str], typer.Option(help="--filter-by-label-name is required for use")
    ] = None,
    filter_by_partner_subscription: Annotated[
        Optional[List[str]], typer.Option()
    ] = None,
    filter_by_requested_service: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_created_start_time: Annotated[
        Optional[str],
        typer.Option(help="YYYY-MM-DDTHH:MM:SSZ"),
    ] = None,
    filter_by_created_end_time: Annotated[
        Optional[str],
        typer.Option(help="YYYY-MM-DDTHH:MM:SSZ"),
    ] = None,
    filter_by_modified_start_time: Annotated[
        Optional[str],
        typer.Option(help="YYYY-MM-DDTHH:MM:SSZ"),
    ] = None,
    filter_by_modified_end_time: Annotated[
        Optional[str],
        typer.Option(help="YYYY-MM-DDTHH:MM:SSZ"),
    ] = None,
    filter_by_tenant_hierarchy: Annotated[Optional[List[str]], typer.Option()] = None,
    sort_by_field: Annotated[TenantOrderField, typer.Option()] = TenantOrderField.ID,
    sort_order: Annotated[OrderDir, typer.Option()] = OrderDir.ASC,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Search Taegis tenants."""
    if filter_by_label_value and not filter_by_label_name:
        raise BadOptionUsage(
            ..., "--filter-by-label-value requires --filter-by-label-name to be set..."
        )

    service = get_service(environment=region, tenant_id=tenant)

    max_results = 1000
    page_number = 1

    log.info(f"Polling page: {page_number}")

    result = service.tenants.query.tenants(
        TenantsQuery(
            max_results=max_results,
            page_num=page_number,
            name=filter_by_name,
            ids=filter_by_tenant,
            for_hierarchies=filter_by_tenant_hierarchy,
            with_partner_subscriptions=filter_by_partner_subscription,
            with_requested_services=filter_by_requested_service,
            label_filter=(
                TenantLabelFilter(
                    label_name=filter_by_label_name, label_value=filter_by_label_value
                )
                if filter_by_label_name
                else None
            ),
            environment_filter=(
                TenantEnvironmentFilter(name=filter_by_region, enabled=True)
                if filter_by_region
                else None
            ),
            created_time_filter=(
                TimeFilter(
                    start_time=filter_by_created_start_time,
                    end_time=filter_by_created_end_time,
                )
                if (filter_by_created_start_time or filter_by_created_end_time)
                else None
            ),
            modified_time_filter=(
                TimeFilter(
                    start_time=filter_by_modified_start_time,
                    end_time=filter_by_modified_end_time,
                )
                if (filter_by_modified_start_time or filter_by_modified_end_time)
                else None
            ),
            with_services=filter_by_service,
            order_by=sort_by_field,
            order_dir=sort_order,
        )
    )

    results = [result]

    while result.has_more:
        page_number += 1
        log.info(f"Polling page: {page_number}")

        result = service.tenants.query.tenants(
            TenantsQuery(
                max_results=max_results,
                page_num=page_number,
                name=filter_by_name,
                ids=filter_by_tenant,
                for_hierarchies=filter_by_tenant_hierarchy,
                with_partner_subscriptions=filter_by_partner_subscription,
                with_requested_services=filter_by_requested_service,
                label_filter=(
                    TenantLabelFilter(
                        label_name=filter_by_label_name,
                        label_value=filter_by_label_value,
                    )
                    if filter_by_label_name
                    else None
                ),
                environment_filter=(
                    TenantEnvironmentFilter(name=filter_by_region, enabled=True)
                    if filter_by_region
                    else None
                ),
                created_time_filter=(
                    TimeFilter(
                        start_time=filter_by_created_start_time,
                        end_time=filter_by_created_end_time,
                    )
                    if (filter_by_created_start_time or filter_by_created_end_time)
                    else None
                ),
                modified_time_filter=(
                    TimeFilter(
                        start_time=filter_by_modified_start_time,
                        end_time=filter_by_modified_end_time,
                    )
                    if (filter_by_modified_start_time or filter_by_modified_end_time)
                    else None
                ),
                with_services=filter_by_service,
                order_by=sort_by_field,
                order_dir=sort_order,
            )
        )

        results.append(result)

    normalized_results = TaegisTenantsResultsNormalizer(
        raw_results=results,
        service="tenants",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


if __name__ == "__main__":
    rv = app(standalone_mode=False)
    print(json.dumps(rv.results))
