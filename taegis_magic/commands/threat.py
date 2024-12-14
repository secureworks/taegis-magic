"""Taegis Magic threat commands."""

import inspect
import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import typer
from dataclasses_json import dataclass_json
from taegis_sdk_python.services.threat.types import ThreatParentType, ThreatPublication
from typing_extensions import Annotated

from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResults, TaegisResultsNormalizer
from taegis_magic.core.service import get_service

log = logging.getLogger(__name__)

app = typer.Typer()
publications_app = typer.Typer()
app.add_typer(publications_app, name="publications")


@dataclass_json
@dataclass
class ThreatPublicationsNormalizer(TaegisResultsNormalizer):
    """Threat Publications Normalizer."""

    raw_results: List[ThreatPublication] = field(default_factory=list)

    @property
    def results(self) -> List[Dict[str, Any]]:
        """Results."""
        service = get_service(environment=self.region)

        # a reference to the threat intel article doesn't exist in the
        # data, so we will generate one based on the region used
        return [
            {
                **asdict(pub),
                "taegis_magic.reference": (
                    f"{service.core.sync_url.replace('api.', '')}/threat-intelligence-publications?"
                    f"headerText=Threat%20Intelligence%20Reports&id={quote(pub.id or '')}"
                ),
            }
            for pub in self.raw_results
        ]


@publications_app.command("latest")
@tracing
def publications_latest(
    size: int,
    region: Optional[str] = None,
):
    """
    Threat Publications Latest.

    Parameters
    ----------
    size : int
        Number of latest threat publications
    region : Optional[str], optional
        Taegis region, by default None

    Returns
    -------
    ThreatPublicationsNormalizer
        ThreatPublicationsNormalizer
    """
    service = get_service(environment=region)

    publications = service.threat.query.threat_latest_publications(from_=0, size=size)

    results = ThreatPublicationsNormalizer(
        service="threat",
        tenant_id=service.tenant_id,
        region=service.environment,
        raw_results=publications,
        arguments={
            "size": size,
            "region": region,
        },
    )

    return results


@publications_app.command("search")
@tracing
def publications_search(
    term: str,
    region: Optional[str] = None,
):
    """
    Threat Publications Search.

    Parameters
    ----------
    term : str
        Search Term
    region : Optional[str], optional
        Taegis Region, by default None

    Returns
    -------
    ThreatPublicationsNormalizer
        ThreatPublicationsNormalizer
    """
    service = get_service(environment=region)

    publications = service.threat.query.threat_publications(text=term)

    results = ThreatPublicationsNormalizer(
        service="threat",
        tenant_id=service.tenant_id,
        region=service.environment,
        raw_results=publications,
        arguments={
            "term": term,
            "region": region,
        },
    )

    return results


@app.command()
@tracing
def watchlist(
    type_: Annotated[ThreatParentType, typer.Option("--type")],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    service = get_service(tenant_id=tenant, environment=region)
    results = service.threat.query.threat_watchlist(type_)

    normalized_results = TaegisResults(
        service="threat",
        tenant_id=service.tenant_id,
        region=service.environment,
        raw_results=results,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results
