"""Taegis Magic tenants commands."""

import inspect
import json
import logging
from dataclasses import asdict, dataclass, field
from typing import List, Optional

import typer
from click.exceptions import BadOptionUsage
from dataclasses_json import dataclass_json
from taegis_sdk_python.services.tenants4.types import (
    TenantResultOrder,
    OrderDir,
    SubscriptionMatcher,
    TenantResults,
    TenantsQuery,
    TenantEnvironment,
    LicenseLevel,
    TenantLicenseCapability,
    TenantLabelMatcher,
)
from typing_extensions import Annotated

from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.service import get_service

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Tenant Commands.")


@dataclass_json
@dataclass
class TaegisTenantsResultsNormalizer(TaegisResultsNormalizer):
    """Taegis TenantResults normalizer."""

    raw_results: List[TenantResults] = field(
        default_factory=lambda: [TenantResults()]
    )

    @property
    def results(self):
        return [
            asdict(tenant) for result in self.raw_results for tenant in result.tenants
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
        Optional[List[str]], typer.Option(help="(supports wildcard *) or tenant id")
    ] = None,
    filter_by_tenant: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_region: Annotated[Optional[TenantEnvironment], typer.Option()] = None,
    filter_by_subscription: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_license_level: Annotated[Optional[LicenseLevel], typer.Option()] = None,
    filter_by_label_name: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_label_value: Annotated[
        Optional[List[str]],
        typer.Option(help="--filter-by-label-name is required for use"),
    ] = None,
    filter_by_partner: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_organization: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_mdr_provider: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_hierarchy: Annotated[Optional[List[str]], typer.Option()] = None,
    filter_by_license_capability_any: Annotated[
        Optional[List[TenantLicenseCapability]], typer.Option()
    ] = None,
    filter_by_license_capability_all: Annotated[
        Optional[List[TenantLicenseCapability]], typer.Option()
    ] = None,
    filter_by_license_capability_none: Annotated[
        Optional[List[TenantLicenseCapability]], typer.Option()
    ] = None,
    sort_by_field: Annotated[TenantResultOrder, typer.Option()] = TenantResultOrder.ID,
    sort_order: Annotated[OrderDir, typer.Option()] = OrderDir.ASC,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Search Taegis tenants."""
    arguments = inspect.currentframe().f_locals

    if filter_by_label_value and not filter_by_label_name:
        raise BadOptionUsage(
            ..., "--filter-by-label-value requires --filter-by-label-name to be set..."
        )

    if len(filter_by_label_name or []) != len(filter_by_label_value or []):
        raise BadOptionUsage(
            ...,
            "--filter-by-label-name and --filter-by-label-value must have the same number of elements",
        )

    service = get_service(environment=region, tenant_id=tenant)

    max_results = 2500

    log.info(f"Polling page: 1")

    result = service.tenants4.query.tenants(
        TenantsQuery(
            count=max_results,
            names=filter_by_name,
            ids=filter_by_tenant,
            hierarchies=filter_by_hierarchy,
            partners=filter_by_partner,
            organizations=filter_by_organization,
            mdr_providers=filter_by_mdr_provider,
            labels_match=(
                [
                    TenantLabelMatcher(name=name, value=value)
                    for name, value in zip(filter_by_label_name, filter_by_label_value)
                ]
                if filter_by_label_name and filter_by_label_value
                else None
            ),
            enabled_in_environments=filter_by_region,
            subscriptions_match=(
                [SubscriptionMatcher(name=name) for name in filter_by_subscription]
                if filter_by_subscription
                else None
            ),
            license_level=filter_by_license_level,
            license_capabilities_any=filter_by_license_capability_any,
            license_capabilities_all=filter_by_license_capability_all,
            license_capabilities_none=filter_by_license_capability_none,
            order_by=sort_by_field,
            order_dir=sort_order,
        )
    )

    results = [result]

    while result.has_more:
        log.info(f"Polling cursor: {result.cursor_pos}")

        result = service.tenants4.query.tenants(
        TenantsQuery(
            after_cursor=result.cursor_pos,
            count=max_results,
            names=filter_by_name,
            ids=filter_by_tenant,
            hierarchies=filter_by_hierarchy,
            partners=filter_by_partner,
            organizations=filter_by_organization,
            mdr_providers=filter_by_mdr_provider,
            labels_match=(
                [
                    TenantLabelMatcher(name=name, value=value)
                    for name, value in zip(filter_by_label_name, filter_by_label_value)
                ]
                if filter_by_label_name and filter_by_label_value
                else None
            ),
            enabled_in_environments=filter_by_region,
            subscriptions_match=(
                [SubscriptionMatcher(name=name) for name in filter_by_subscription]
                if filter_by_subscription
                else None
            ),
            license_level=filter_by_license_level,
            license_capabilities_any=filter_by_license_capability_any,
            license_capabilities_all=filter_by_license_capability_all,
            license_capabilities_none=filter_by_license_capability_none,
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
        arguments=arguments,
    )

    return normalized_results


if __name__ == "__main__":
    rv = app(standalone_mode=False)
    print(json.dumps(rv.results))
