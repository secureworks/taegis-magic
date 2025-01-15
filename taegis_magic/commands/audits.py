"""Taegis Magic audits commands."""

import inspect
import logging
from dataclasses import asdict, dataclass, field
from pprint import pprint
from typing import Any, Dict, List, Optional

import typer
from dataclasses_json import config, dataclass_json
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.service import get_service
from taegis_sdk_python.services.audits.types import (
    AllAuditsInput,
    Audit,
    AuditEventEnum,
    AuditSearchInput,
    SortBy,
    SortOrder,
    AuditResult,
    AuditEventResult,
)
from typing_extensions import Annotated

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Audit Commands.")


@dataclass_json
@dataclass
class TaegisAuditNormalizer(TaegisResultsNormalizer):
    """Signle Taegis Audit Results Normalizer."""

    raw_results: Audit = field(default_factory=lambda: Audit())

    @property
    def results(self):
        return [asdict(self.raw_results)]


@dataclass_json
@dataclass
class TaegisAuditResultNormalizer(TaegisResultsNormalizer):
    """Taegis AuditResult Results Normalizer."""

    raw_results: AuditResult = field(default_factory=lambda: AuditResult())

    @property
    def results(self):
        return [asdict(audit) for audit in self.raw_results.audits or []]

    @property
    def status(self) -> str:
        """Status of query.

        Returns
        -------
        str
            Status message of query.
        """
        return "SUCCESS" if self.raw_results else "ERROR"

    @property
    def total_results(self) -> int:
        # This signifies an error
        if not self.raw_results:
            return -1

        # This signifies a protobuf error, where 0 results doesn't return a total_results value
        return self.raw_results.total_results or -1

    @property
    def results_returned(self) -> int:
        """Query results total number of results returned.

        Returns
        -------
        str
            Returns number of results.
        """
        return len(self.raw_results.audits) if self.raw_results else -1


@dataclass_json
@dataclass
class TaegisAuditEventResultNormalizer(TaegisResultsNormalizer):
    """Taegis AuditResult Results Normalizer."""

    raw_results: AuditEventResult = field(default_factory=lambda: AuditEventResult())

    @property
    def results(self):
        return [asdict(event) for event in self.raw_results.audit_events or []]

    @property
    def status(self) -> str:
        """Status of query.

        Returns
        -------
        str
            Status message of query.
        """
        return "SUCCESS" if self.raw_results else "ERROR"

    @property
    def total_results(self) -> int:
        # This signifies an error
        if not self.raw_results:
            return -1

        # This signifies a protobuf error, where 0 results doesn't return a total_results value
        return self.raw_results.total_events or -1

    @property
    def results_returned(self) -> int:
        """Query results total number of results returned.

        Returns
        -------
        str
            Returns number of results.
        """
        return len(self.raw_results.audit_events) if self.raw_results else -1


@app.command(name="audit")
@tracing
def audits_audit(
    audit_id: Annotated[str, typer.Option()],
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis audits get a single audit record."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.audits.query.audit(audit_id=audit_id)

    normalized_results = TaegisAuditNormalizer(
        raw_results=results,
        service="audits",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="all")
@tracing
def audits_all(
    before: Annotated[Optional[str], typer.Option(help="YYYY-DD-MMTHH:MM:SSZ")] = None,
    after: Annotated[Optional[str], typer.Option(help="YYYY-DD-MMTHH:MM:SSZ")] = None,
    sort_order: Annotated[Optional[SortOrder], typer.Option()] = None,
    sort_by: Annotated[Optional[SortBy], typer.Option()] = None,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Get all audits."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.audits.query.all_audits(
        AllAuditsInput(
            offset=0,
            limit=10000,
            before=before,
            after=after,
            sort_order=sort_order,
            sort_by=sort_by,
        )
    )

    normalized_results = TaegisAuditResultNormalizer(
        raw_results=results,
        service="audits",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="search")
@tracing
def audits_search(
    audit_id: Annotated[Optional[str], typer.Option()] = None,
    log_type: Annotated[Optional[str], typer.Option()] = None,
    application: Annotated[Optional[str], typer.Option()] = None,
    request_type: Annotated[Optional[str], typer.Option()] = None,
    username: Annotated[Optional[str], typer.Option()] = None,
    email: Annotated[Optional[str], typer.Option()] = None,
    source: Annotated[Optional[str], typer.Option()] = None,
    target_rn: Annotated[Optional[str], typer.Option()] = None,
    action: Annotated[Optional[str], typer.Option()] = None,
    event_name: Annotated[Optional[str], typer.Option()] = None,
    event_desc: Annotated[Optional[str], typer.Option()] = None,
    trace_id: Annotated[Optional[str], typer.Option()] = None,
    url: Annotated[Optional[str], typer.Option()] = None,
    search_by_all: Annotated[Optional[str], typer.Option()] = None,
    before: Annotated[Optional[str], typer.Option(help="YYYY-DD-MMTHH:MM:SSZ")] = None,
    after: Annotated[Optional[str], typer.Option(help="YYYY-DD-MMTHH:MM:SSZ")] = None,
    sort_order: Annotated[Optional[SortOrder], typer.Option()] = None,
    sort_by: Annotated[Optional[SortBy], typer.Option()] = None,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Search for audits."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.audits.query.search_audits(
        AuditSearchInput(
            offset=0,
            limit=10000,
            id=audit_id,
            log_type=log_type,
            application=application,
            request_type=request_type,
            username=username,
            email=email,
            source=source,
            target_rn=target_rn,
            action=action,
            event_name=event_name,
            event_desc=event_desc,
            trace_id=trace_id,
            url=url,
            search_by_all=search_by_all,
            before=before,
            after=after,
            sort_by=sort_by,
            sort_order=sort_order,
        )
    )

    normalized_results = TaegisAuditResultNormalizer(
        raw_results=results,
        service="audits",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="application-events")
@tracing
def audits_application_events(
    event_type: Annotated[AuditEventEnum, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Get event name list."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.audits.query.list_application_events(application=event_type)

    normalized_results = TaegisAuditEventResultNormalizer(
        raw_results=results,
        service="audits",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results
