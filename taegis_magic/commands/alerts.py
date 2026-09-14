"""Taegis Magic alerts commands."""

import logging
from dataclasses import asdict, dataclass, field
from functools import partial
from pprint import pprint
from typing import Annotated, Any, Dict, List, Optional

import typer
from dataclasses_json import config, dataclass_json
from taegis_sdk_python import (
    GraphQLNoRowsInResultSetError,
    GraphQLService,
    build_output_string,
    prepare_input,
)
from taegis_sdk_python.commons import execute_delimited_queries
from taegis_sdk_python.config import get_config
from taegis_sdk_python.services.alerts.types import (
    Alert2,
    AlertsList,
    AlertsResponse,
    AuxiliaryEvent,
    PollRequestInput,
    SearchRequestInput,
)
from taegis_sdk_python.services.llm_service.types import NLSearchInputsV2
from taegis_sdk_python.services.sharelinks.types import (
    ExtraParamCreateInput,
    ShareLinkCreateInput,
)
from taegis_magic.commands.configure import QUERIES_SECTION
from taegis_magic.commands.utils.investigations import insert_search_query
from taegis_magic.commands.utils.nl_queries import insert_nl_search_query
from taegis_magic.core.log import tracing
from taegis_magic.core.macros import resolve_tenants
from taegis_magic.core.normalizer import (
    TaegisResults,
    TaegisResultsNormalizer,
    merge_normalizer_results,
)
from taegis_magic.core.service import get_service
from taegis_magic.core.time_chunks import render_time_chunked_queries

log = logging.getLogger(__name__)

CONFIG = get_config()
if not CONFIG.has_section(QUERIES_SECTION):
    CONFIG.add_section(QUERIES_SECTION)

app = typer.Typer(help="Taegis Alerts Commands.")


@dataclass_json
@dataclass
class AlertsResultsNormalizer(TaegisResultsNormalizer):
    """Taegis Alerts Normalizer."""

    raw_results: List[AlertsResponse] = field(default_factory=list)
    query: str = ""
    is_saved: bool = False
    _query_id: Optional[str] = None
    _shareable_url: Optional[str] = None

    def _repr_markdown_(self):
        """Represent as markdown."""
        return self._display_template(
            "taegis_search_results.md.jinja"
        )  # pragma: no test

    @property
    def results(self) -> List[Dict[str, Any]]:
        log.debug("Calling AlertsResultsNormalizer.results...")
        if not self.raw_results:
            return []

        if self.raw_results[0].alerts.group_by:
            return self.aggregate

        return [
            asdict(alert) for result in self.raw_results or [] for alert in result.alerts.list_ or []
        ]

    @property
    def status(self) -> str:
        """Alerts Service results status."""
        log.debug("Calling AlertsResultsNormalizer.status...")
        return self.raw_results[0].status.value if self.raw_results else "Error"

    @property
    def total_results(self) -> int:
        log.debug("Calling AlertsResultsNormalizer.num_total...")
        # This signifies an error
        if not self.raw_results:
            return -1

        # This signifies a protobuf error, where 0 results doesn't return a total_results value
        return (
            self.raw_results[0].alerts.total_results
            if self.raw_results[0].alerts.total_results
            else 0
        )

    @property
    def results_returned(self) -> int:
        """Query results total number of results returned.

        Returns
        -------
        str
            Returns number of results.
        """
        log.debug("Calling AlertsResultsNormalizer.total_returned...")
        if not self.raw_results:
            return -1

        return sum(
            len(result.alerts.list_ or [])
            for result in self.raw_results
            if result.alerts is not None
        )

    @property
    def aggregate(self) -> List[Dict[str, Any]]:
        """
        Taegis Alerts aggregate results parser.

        Returns
        -------
        List[Dict[str, Any]]
            List of aggregate results
        """
        log.debug("Calling AlertsResultsNormalizer.aggregate...")
        if not self.raw_results[0].alerts.group_by:
            return []

        aggs = []
        for response in self.raw_results[0].alerts.group_by:
            agg = {key.key: key.value for key in response.keys}
            agg["count"] = response.value
            aggs.append(agg)

        return aggs

    @property
    def query_identifier(self) -> Optional[str]:
        """Generate a query identifier for Taegis XDR QL Event queries.

        Returns
        -------
        str
            Query Identifier

        Raises
        ------
        ValueError
            No query found to generate query id
        ValueError
            No query id returned from Query API
        """
        if not self.raw_results:
            return None

        ids = set()
        for result in self.raw_results:
            if result.query_id:
                ids.add(result.query_id)

        if len(ids) == 1:
            return list(ids)[0]

        if len(ids) > 1:
            return '\n'.join(ids)

        return None

    def _create_share_link(self, query_id: str) -> str:
        """Create a Sharelinks URL for a given query identifier."""
        service = get_service(environment=self.region, tenant_id=self.tenant_id)

        result = service.sharelinks.mutation.create_share_link(
            ShareLinkCreateInput(
                link_ref=query_id,
                link_target="cql",
                link_type="queryId",
                tenant_id=self.tenant_id,
                extra_parameters=[
                    ExtraParamCreateInput(key="sourceType", value="alert"),
                ],
            )
        )

        return f'{service.core.sync_url.replace("api.", "")}/share/{result.id_}'

    @property
    def shareable_url(self) -> str:
        """Alerts Service Sharelinks URL."""
        log.debug("Calling AlertsResultsNormalizer.shareable_url...")
        if self._shareable_url:
            return self._shareable_url

        if not self.raw_results:
            return "Unable to create shareable link"

        if self.aggregate:
            return "Unable to create shareable link"

        if not self.query_identifier:
            return "Unable to create shareable link"

        self._shareable_url = self._create_share_link(self.query_identifier)
        return self._shareable_url


@dataclass_json
@dataclass
class ChunkedAlertsResultsNormalizer(AlertsResultsNormalizer):
    """Taegis Alerts Normalizer for time chunked searches."""

    @property
    def chunk_results(self) -> List[AlertsResponse]:
        """First response of each time chunk.

        Each time chunk search returns part 1 (part 0 when there are no
        results); the polled parts of a chunk return part 2 and above.
        """
        log.debug("Calling ChunkedAlertsResultsNormalizer.chunk_results...")
        return [
            response
            for response in self.raw_results
            if response.alerts is not None
            and (response.alerts.part is None or response.alerts.part <= 1)
        ]

    @property
    def chunks(self) -> int:
        """Number of time chunks returning results."""
        log.debug("Calling ChunkedAlertsResultsNormalizer.chunks...")
        return len(self.chunk_results)

    @property
    def chunk_total_results(self) -> int:
        """Sum of the total results reported by each time chunk."""
        log.debug("Calling ChunkedAlertsResultsNormalizer.chunk_total_results...")
        return sum(
            response.alerts.total_results or 0 for response in self.chunk_results
        )

    @property
    def total_results(self) -> int:
        log.debug("Calling ChunkedAlertsResultsNormalizer.total_results...")
        # This signifies an error
        if not self.raw_results:
            return -1

        return self.chunk_total_results

    @property
    def shareable_url(self) -> str:
        """Alerts Service Sharelinks URL."""
        log.debug("Calling ChunkedAlertsResultsNormalizer.shareable_url...")
        if self._shareable_url:
            return self._shareable_url

        # each time chunk has its own query identifier and shareable link
        if self.chunks > 1:
            if not self.raw_results or self.aggregate:
                return "Unable to create shareable link"

            urls = [
                self._create_share_link(response.query_id)
                for response in self.chunk_results
                if response.query_id
            ]

            if not urls:
                return "Unable to create shareable link"

            self._shareable_url = "\n".join(urls)
            return self._shareable_url

        return super().shareable_url


@dataclass_json
@dataclass(order=True, eq=True, frozen=True)
class CustomAuxiliaryEvent(AuxiliaryEvent):
    """My Custom Auxiliary Event - Extends Auxiliary Event with event_data
    to take advantage of GQL federated services.
    """

    event_data: Optional[Dict[str, Any]] = field(
        default=None, metadata=config(field_name="event_data")
    )


@dataclass_json
@dataclass(order=True, eq=True, frozen=True)
class CustomAlert2(Alert2):
    """My Custom Alert2."""

    event_ids: Optional[List[CustomAuxiliaryEvent]] = field(
        default=None, metadata=config(field_name="event_ids")
    )


@dataclass_json
@dataclass(order=True, eq=True, frozen=True)
class CustomAlertsList(AlertsList):
    """My Custom AlertsList."""

    list_: Optional[list[CustomAlert2]] = field(
        default=None, metadata=config(field_name="list")
    )


@dataclass_json
@dataclass(order=True, eq=True, frozen=True)
class CustomAlertsResponse(AlertsResponse):
    """My Custom AlertsResponse."""

    alerts: Optional[CustomAlertsList] = field(
        default=None, metadata=config(field_name="alerts")
    )


@tracing
def alerts_service_search_with_events(
    service: GraphQLService, in_: SearchRequestInput
) -> CustomAlertsResponse:
    """Query Taegis Alerts with corresponding Events attached."""
    endpoint = "alertsServiceSearch"
    result = service.alerts.execute_query(
        endpoint=endpoint,
        variables={
            "in": prepare_input(in_),
        },
        output=build_output_string(CustomAlertsResponse),
    )
    if result is not None:
        return CustomAlertsResponse.from_dict(result.get(endpoint))
    raise GraphQLNoRowsInResultSetError("for query alertsServiceSearch")


@tracing
def alerts_service_poll_with_events(
    service: GraphQLService, in_: PollRequestInput
) -> CustomAlertsResponse:
    endpoint = "alertsServicePoll"
    result = service.alerts.execute_query(
        endpoint=endpoint,
        variables={
            "in": prepare_input(in_),
        },
        output=build_output_string(CustomAlertsResponse),
    )
    if result is not None:
        return CustomAlertsResponse.from_dict(result.get(endpoint))
    raise GraphQLNoRowsInResultSetError("for custom query alertsServicePoll")


def _search_single_tenant(
    cell: str,
    region: Optional[str],
    tenant_id: Optional[str],
    limit: int,
    graphql_output: Optional[str],
    ai: bool = False,
    database: str = ":memory:",
) -> AlertsResultsNormalizer:
    """Execute an alerts search against a single tenant."""
    service = get_service(environment=region, tenant_id=tenant_id)

    if ai:
        llm_results = service.llm_service.query.nl_search_v2(
            in_=NLSearchInputsV2(
                query=cell,
            )
        )
        log.info(f"LLM Search Results: {llm_results}")

        if not llm_results.ql:
            raise ValueError("LLM did not return a query. Cannot proceed with search.")

        if (
            "from alert" not in llm_results.ql.lower()
            and "from detection" not in llm_results.ql.lower()
        ):
            raise ValueError(
                "LLM did not return a query targeting the alerts service. Cannot proceed with search."
            )

        insert_nl_search_query(database, cell, llm_results)

        cell = llm_results.ql

    with service(output=graphql_output):
        result = alerts_service_search_with_events(
            service,
            SearchRequestInput(
                cql_query=cell,
                offset=0,
                limit=limit,
                metadata={
                    "callerName": CONFIG[QUERIES_SECTION].get(
                        "callername", fallback="Taegis Magic"
                    ),
                },
            ),
        )

    poll_responses = [result]
    search_id = result.search_id
    total_parts = result.alerts.total_parts

    if search_id:
        for part in range(2, total_parts + 1):
            response = None
            try:
                log.debug(f"Submitting page {part}...")
                with service(output=graphql_output):
                    response = alerts_service_poll_with_events(
                        service,
                        PollRequestInput(
                            search_id=search_id,
                            part_id=part,
                        ),
                    )
            except Exception as exc:
                log.error(
                    f"Cannot retrieve results for search_id:{search_id}:{part}::{exc}"
                )
                if "not found" in str(exc):
                    break

            if isinstance(response, AlertsResponse) and response.alerts is not None:
                poll_responses.append(response)
                # CX-92571 work around
                if sum(
                    len(response.alerts.list_) for response in poll_responses
                ) >= int(limit):
                    break

    return AlertsResultsNormalizer(
        raw_results=poll_responses,
        service="alerts",
        tenant_id=service.tenant_id,
        region=service.environment,
        query=cell,
        arguments={
            "cell": cell,
            "region": service.environment,
            "tenant": service.tenant_id,
            "limit": limit,
            "graphql_output": graphql_output,
        },
    )


def _search_single_tenant_time_chunked(
    cell: str,
    region: Optional[str],
    tenant_id: Optional[str],
    limit: int,
    graphql_output: Optional[str],
    time_window: str,
    time_chunk: str,
) -> ChunkedAlertsResultsNormalizer:
    """Execute a time chunked alerts search against a single tenant."""
    queries = render_time_chunked_queries(cell, time_window, time_chunk)
    log.debug(f"Time chunked queries::{queries}")

    service = get_service(environment=region, tenant_id=tenant_id)

    chunk_normalizers, errors = execute_delimited_queries(
        queries,
        partial(
            _search_single_tenant,
            region=region,
            tenant_id=tenant_id,
            limit=limit,
            graphql_output=graphql_output,
        ),
        error_handling="partial",
    )

    for error in errors:
        log.error(
            f"Cannot retrieve results for time chunk::{error.item}::{error.error}"
        )

    return ChunkedAlertsResultsNormalizer(
        raw_results=[
            response
            for normalizer in chunk_normalizers
            for response in normalizer.raw_results
        ],
        service="alerts",
        tenant_id=service.tenant_id,
        region=service.environment,
        query=queries,
        arguments={
            "cell": cell,
            "region": service.environment,
            "tenant": service.tenant_id,
            "limit": limit,
            "graphql_output": graphql_output,
            "time_window": time_window,
            "time_chunk": time_chunk,
        },
    )


@app.command()
@tracing
def search(
    cell: Optional[str] = None,
    region: Optional[str] = None,
    tenant: Optional[str] = None,
    limit: int = 10000,
    graphql_output: Optional[str] = None,
    track: Annotated[bool, typer.Option()] = CONFIG[QUERIES_SECTION].getboolean(
        "track", fallback=False
    ),
    database: Annotated[str, typer.Option()] = ":memory:",
    ai: Annotated[bool, typer.Option()] = False,
    time_window: Annotated[
        Optional[str],
        typer.Option(
            help="Total duration to search (i.e. 30d), split into --time-chunk queries."
        ),
    ] = None,
    time_chunk: Annotated[
        Optional[str],
        typer.Option(help="Duration of each time chunk query (i.e. 7d)."),
    ] = None,
) -> Optional[AlertsResultsNormalizer]:
    """
    Search Taegis Alerts service.

    Supports @macro syntax in --tenant to search across multiple tenants.

    --time-window and --time-chunk must be set together to search a total
    duration as concurrent time chunked queries.  Durations are expressed as
    a number and a unit (s, m, h, d, w, mo, y).  The query cannot set its own
    EARLIEST/LATEST; they are appended, unless the query already contains the
    `EARLIEST='{{ window.earliest }}' LATEST='{{ window.latest }}'` placeholders.
    """
    if not cell:
        cell = ""

    if "aggregate" in cell:
        limit = 1

    if bool(time_window) != bool(time_chunk):
        raise ValueError("--time-window and --time-chunk must be set together.")

    if time_window and ai:
        raise ValueError("--ai cannot be used with time chunked searches.")

    tenant_ids = resolve_tenants(tenant, region)

    all_results: List[AlertsResultsNormalizer]
    if time_window and time_chunk:
        all_results = [
            _search_single_tenant_time_chunked(
                cell, region, tid, limit, graphql_output, time_window, time_chunk
            )
            for tid in tenant_ids
        ]
    else:
        all_results = [
            _search_single_tenant(
                cell, region, tid, limit, graphql_output, ai, database
            )
            for tid in tenant_ids
        ]

    if len(all_results) == 1:
        results = all_results[0]
    else:
        results = merge_normalizer_results(all_results)

    if track:
        insert_search_query(database, results)

    return results


@app.command()
@tracing
def history(
    id_: Annotated[str, typer.Option("--id", help="Alert ID")],
    tenant: Annotated[Optional[str], typer.Option(help="Taegis Tenant ID")] = None,
    region: Annotated[Optional[str], typer.Option(help="Taegis Region")] = None,
) -> TaegisResults:
    """Retrieve Alert history by ID."""
    service = get_service(tenant_id=tenant, environment=region)

    results = service.alerts_history.query.alert_history_by_id(id_=id_)

    return TaegisResults(
        raw_results=results,
        service="alerts_history",
        tenant_id=service.tenant_id,
        region=service.environment,
    )


if __name__ == "__main__":
    rv = app(standalone_mode=False)
    pprint(rv)
