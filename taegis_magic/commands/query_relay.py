"""Taegis Magic query relay commands."""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import typer
from dataclasses_json import dataclass_json
from gql.transport.exceptions import TransportQueryError
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.service import get_service
from taegis_sdk_python.services.query_relay.types import (
    EnrichOptions,
    ExecuteQueryRelayInput,
    FetchQueryRelayResultsInput,
    QueryRelayResult,
    QueryRelayStatus,
)
from typing_extensions import Annotated

try:
    from tqdm.auto import tqdm
except ImportError:
    tqdm = None

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Query Commands.")

POLL_INTERVAL_SECONDS = 2


def _get_bar(total, desc, unit, disable=False):
    if tqdm is not None:
        kwargs = dict(desc=desc, unit=unit, disable=disable)
        if total is not None:
            kwargs["total"] = total
        else:
            kwargs["bar_format"] = "{desc}: {n_fmt} {unit} [{elapsed}, {rate_fmt}]"
        return tqdm(**kwargs)
    return None


def _transport_error_details(error: TransportQueryError) -> Dict[str, Any]:
    """Extract the first GraphQL error from a transport exception."""
    payload = error.args[0] if error.args else {}
    errors = getattr(error, "errors", None)
    if not errors:
        errors = payload if isinstance(payload, list) else [payload]
    graphql_error = errors[0] if errors and isinstance(errors[0], dict) else {}
    extensions = graphql_error.get("extensions") or {}
    return {
        "message": graphql_error.get("message") or str(error),
        "path": ".".join(str(part) for part in graphql_error.get("path", [])),
        "error_type": extensions.get("serviceName"),
        "code": extensions.get("code"),
    }


@dataclass_json
@dataclass
class TaegisQueryRelayNormalizer(TaegisResultsNormalizer):
    """Taegis Query Relay Normalizer."""

    raw_results: List[Dict[str, Any]] = field(default_factory=list)
    correlation_id: Optional[str] = None
    status: Optional[str] = None
    result: Optional[str] = None
    error_type: Optional[str] = None
    message: Optional[str] = None
    code: Optional[str] = None
    path: Optional[str] = None
    is_error: bool = False

    @property
    def results(self) -> List[Dict[str, Any]]:
        return self.raw_results

    def _repr_markdown_(self):
        """Represent as markdown."""
        return self._display_template("taegis_query_relay_results.md.jinja")


@app.command()
@tracing
def search(
    cell: Annotated[str, typer.Option(help="SQL query to execute")],
    workload: Annotated[
        Optional[str],
        typer.Option(help="QEE workload type for Trino cluster routing (optional; QEE falls back to a default when omitted)"),
    ] = None,
    time_range_start: Annotated[
        Optional[str], typer.Option(help="ISO8601 start time")
    ] = None,
    time_range_end: Annotated[
        Optional[str], typer.Option(help="ISO8601 end time")
    ] = None,
    page_size: Annotated[int, typer.Option(help="Results per page")] = 1000,
    no_enrich_hostname: Annotated[
        bool,
        typer.Option(
            "--no-enrich-hostname",
            help="Disable host_id to $hostname enrichment (enabled by default)",
        ),
    ] = False,
    tenant: Annotated[Optional[str], typer.Option(help="Tenant ID")] = None,
    region: Annotated[Optional[str], typer.Option(help="Taegis Region")] = None,
    progress: Annotated[bool, typer.Option(help="Show progress bars")] = True,
):
    """Submit a SQL query via Query Relay and return all results."""
    service = get_service(environment=region, tenant_id=tenant)
    show_progress = progress and tqdm is not None

    # Submit the query and get a token back
    try:
        response = service.query_relay.mutation.execute_query_relay(
            ExecuteQueryRelayInput(
                sql=cell,
                time_range_start=time_range_start,
                time_range_end=time_range_end,
                workload=workload,
            )
        )
    except TransportQueryError as exc:
        error_details = _transport_error_details(exc)
        log.error(
            "Query Relay validation failed: message=%s code=%s path=%s",
            error_details["message"],
            error_details["code"],
            error_details["path"],
        )
        return TaegisQueryRelayNormalizer(
            service="query_relay",
            tenant_id=service.tenant_id,
            region=service.environment,
            status="ERROR",
            result="FAILED",
            error_type=error_details["error_type"],
            message=error_details["message"],
            code=error_details["code"],
            path=error_details["path"],
            is_error=True,
        )
    token = response.token
    if response.error:
        error = response.error
        message = (
            f"Query Relay execution failed to start: {response.status} "
            f"error={error.error} message={error.message} code={error.code} "
            f"correlationId={response.correlation_id}"
        )
        log.error(message)
        return TaegisQueryRelayNormalizer(
            service="query_relay",
            tenant_id=service.tenant_id,
            region=service.environment,
            correlation_id=response.correlation_id,
            status=response.status.value if response.status else None,
            error_type=error.error,
            message=error.message,
            code=error.code,
            is_error=True,
        )
    log.info(f"Query submitted, token: {token}, correlationId: {response.correlation_id}")

    enrich = EnrichOptions(hostname=not no_enrich_hostname)

    # Phase 1: Poll until execution completes
    poll_bar = _get_bar(
        total=None, desc="Executing query", unit="poll", disable=not show_progress
    )
    try:
        while True:
            poll_response = service.query_relay.query.fetch_query_relay_results(
                FetchQueryRelayResultsInput(
                    token=token, page_size=page_size, enrich=enrich
                )
            )
            if poll_response.status == QueryRelayStatus.FINISHED:
                if poll_bar is not None:
                    poll_bar.set_description("Query finished")
                    poll_bar.update(1)
                break
            if poll_bar is not None:
                poll_bar.set_description(f"Waiting ({poll_response.status.value})")
                poll_bar.update(1)
            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        if poll_bar is not None:
            poll_bar.close()

    if poll_response.result != QueryRelayResult.SUCCEEDED:
        error = poll_response.error
        message = (
            f"Query Relay execution did not succeed: {poll_response.result} "
            f"error={error.error if error else None} "
            f"message={error.message if error else None} "
            f"code={error.code if error else None} "
            f"correlationId={poll_response.correlation_id}"
        )
        log.error(message)
        return TaegisQueryRelayNormalizer(
            service="query_relay",
            tenant_id=service.tenant_id,
            region=service.environment,
            correlation_id=poll_response.correlation_id,
            status=poll_response.status.value if poll_response.status else None,
            result=poll_response.result.value if poll_response.result else None,
            error_type=error.error if error else None,
            message=error.message if error else None,
            code=error.code if error else None,
            is_error=True,
        )

    # Phase 2: Fetch all pages with progress
    total_rows = poll_response.pages.total if poll_response.pages else None
    all_rows: List[Dict[str, Any]] = list(poll_response.rows or [])
    next_key = poll_response.pages.next_key if poll_response.pages else None

    fetch_bar = _get_bar(
        total=total_rows, desc="Fetching results", unit="rows", disable=not show_progress
    )
    try:
        if fetch_bar is not None:
            fetch_bar.update(len(all_rows))

        while next_key:
            page = service.query_relay.query.fetch_query_relay_results(
                FetchQueryRelayResultsInput(
                    token=token,
                    page_size=page_size,
                    next_page_token=next_key,
                    enrich=enrich,
                )
            )
            page_rows = page.rows or []
            all_rows.extend(page_rows)
            if fetch_bar is not None:
                fetch_bar.update(len(page_rows))
            next_key = page.pages.next_key if page.pages else None
    finally:
        if fetch_bar is not None:
            fetch_bar.close()

    return TaegisQueryRelayNormalizer(
        raw_results=all_rows,
        correlation_id=poll_response.correlation_id,
        status=poll_response.status.value if poll_response.status else None,
        result=poll_response.result.value if poll_response.result else None,
        service="query_relay",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments={
            "cell": cell,
            "time_range_start": time_range_start,
            "time_range_end": time_range_end,
            "workload": workload,
            "page_size": page_size,
            "no_enrich_hostname": no_enrich_hostname,
            "tenant": tenant,
            "region": region,
        },
    )
