"""Taegis Magic query relay commands."""

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import typer
from dataclasses_json import dataclass_json
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.service import get_service
from taegis_sdk_python import ServiceCoreException
from taegis_sdk_python.services.query_relay.types import (
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


@dataclass_json
@dataclass
class TaegisQueryRelayNormalizer(TaegisResultsNormalizer):
    """Taegis Query Relay Normalizer."""

    raw_results: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def results(self) -> List[Dict[str, Any]]:
        return self.raw_results


@app.callback(invoke_without_command=True)
@tracing
def search(
    cell: Annotated[str, typer.Option(help="SQL query to execute")],
    workload: Annotated[str, typer.Option(help="QEE workload type for Trino cluster routing")],
    time_range_start: Annotated[
        Optional[str], typer.Option(help="ISO8601 start time")
    ] = None,
    time_range_end: Annotated[
        Optional[str], typer.Option(help="ISO8601 end time")
    ] = None,
    page_size: Annotated[int, typer.Option(help="Results per page")] = 1000,
    tenant: Annotated[Optional[str], typer.Option(help="Tenant ID")] = None,
    region: Annotated[Optional[str], typer.Option(help="Taegis Region")] = None,
    progress: Annotated[bool, typer.Option(help="Show progress bars")] = True,
):
    """Submit a SQL query via Query Relay and return all results."""
    service = get_service(environment=region, tenant_id=tenant)
    show_progress = progress and tqdm is not None

    # Submit the query and get a token back
    response = service.query_relay.mutation.execute_query_relay(
        ExecuteQueryRelayInput(
            sql=cell,
            time_range_start=time_range_start,
            time_range_end=time_range_end,
            workload=workload,
        )
    )
    token = response.token
    if response.error:
        error = response.error
        message = (
            f"Query Relay execution failed to start: {response.status} "
            f"error={error.error} message={error.message} code={error.code}"
        )
        log.error(message)
        raise ServiceCoreException(message)
    log.info(f"Query submitted, token: {token}")

    # Phase 1: Poll until execution completes
    poll_bar = _get_bar(
        total=None, desc="Executing query", unit="poll", disable=not show_progress
    )
    try:
        while True:
            poll_response = service.query_relay.query.fetch_query_relay_results(
                FetchQueryRelayResultsInput(token=token, page_size=page_size)
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
            f"code={error.code if error else None}"
        )
        log.error(message)
        raise ServiceCoreException(message)

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
        service="query_relay",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments={
            "cell": cell,
            "time_range_start": time_range_start,
            "time_range_end": time_range_end,
            "workload": workload,
            "page_size": page_size,
            "tenant": tenant,
            "region": region,
        },
    )
