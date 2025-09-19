"""Taegis Magic Sharelinks Commands."""

import logging
from typing import Optional
from urllib.parse import urlparse

import typer
from taegis_magic.commands.alerts import AlertsResultsNormalizer
from taegis_magic.commands.events import TaegisEventNormalizer
from taegis_magic.commands.investigations import InvestigationsCreatedResultsNormalizer
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.service import get_service
from taegis_sdk_python.services.alerts.types import GetByIDRequestInput
from taegis_sdk_python.services.investigations2.types import InvestigationV2Arguments
from typing_extensions import Annotated

app = typer.Typer(help="Taegis Sharelinks Commands.")

log = logging.getLogger(__name__)


@app.command()
@tracing
def unfurl(
    id_: Annotated[str, typer.Option("--id", help="Taegis Sharelink URL or UUID")],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
) -> TaegisResultsNormalizer:
    """Unpack sharelink urls and uuids.  Returns underlying datastructure results."""
    if "/share/" in id_:
        parse_result = urlparse(id_)
        id_ = parse_result.path.replace("/share/", "")

    service = get_service(environment=region, tenant_id=tenant)

    results = service.sharelinks.query.share_link_by_id(id_=id_)

    if results.link_type == "alertId":
        with service(tenant_id=results.tenant_id):
            unfurl_results = service.alerts.query.alerts_service_retrieve_alerts_by_id(
                GetByIDRequestInput(i_ds=[results.link_ref])
            )

        normalized_results = AlertsResultsNormalizer(
            raw_results=[unfurl_results],
            service="alerts",
            tenant_id=results.tenant_id,
            region=service.environment,
            arguments={
                "id": id_,
                "region": service.environment,
                "tenant": service.tenant_id,
            },
        )
        normalized_results._shareable_url = (
            f'{service.core.sync_url.replace("api.", "")}/share/{id_}'
        )
    elif results.link_type == "eventId":
        with service(tenant_id=results.tenant_id):
            unfurl_results = service.events.query.events(ids=[results.link_ref])
            normalized_results = TaegisEventNormalizer(
                raw_results=unfurl_results,
                service="events",
                tenant_id=service.tenant_id,
                region=service.environment,
                arguments={
                    "id": id_,
                    "region": service.environment,
                    "tenant": service.tenant_id,
                },
            )
    elif results.link_type == "investigationId":
        with service(tenant_id=results.tenant_id):
            unfurl_results = service.investigations2.query.investigation_v2(
                InvestigationV2Arguments(id=results.link_ref)
            )
            normalized_results = InvestigationsCreatedResultsNormalizer(
                raw_results=unfurl_results,
                service="cases",
                tenant_id=service.tenant_id,
                region=service.environment,
                arguments={
                    "id": id_,
                    "region": service.environment,
                    "tenant": service.tenant_id,
                },
            )
    else:
        raise ValueError(f"Unable to process sharelink results: {results}")

    return normalized_results
