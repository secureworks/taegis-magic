"""Taegis Magic alerts commands."""

import logging
from dataclasses import asdict, dataclass, field
from pprint import pprint
from typing import Any, Dict, List, Optional

import typer
from dataclasses_json import config, dataclass_json
from taegis_magic.commands.configure import QUERIES_SECTION
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer, TaegisResults

from taegis_magic.core.service import get_service
from taegis_sdk_python import (
    GraphQLNoRowsInResultSetError,
    GraphQLService,
    build_output_string,
    prepare_input,
)
from taegis_sdk_python.config import get_config
from taegis_sdk_python.services.alerts.types import (
    Alert2,
    AlertsList,
    AlertsResponse,
    AuxiliaryEvent,
    PollRequestInput,
    SearchRequestInput,
)
from taegis_sdk_python.services.sharelinks.types import (
    ExtraParamCreateInput,
    ShareLinkCreateInput,
)
from taegis_magic.commands.utils.investigations import insert_search_query
from typing_extensions import Annotated


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
            asdict(alert) for result in self.raw_results for alert in result.alerts.list
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
        return (
            sum([len(result.alerts.list) for result in self.raw_results])
            if self.raw_results
            else -1
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
        """Alerts Service Query Identifier."""
        log.debug("Calling AlertsResultsNormalizer.query_identifier...")
        if not self.raw_results:
            return None

        if self.raw_results[0].query_id:
            return self.raw_results[0].query_id

        return None

    @property
    def shareable_url(self) -> str:
        """Alerts Service Sharelinks URL."""
        log.debug("Calling AlertsResultsNormalizer.shareable_url...")
        if not self.raw_results:
            return "Unable to create shareable link"

        if self.aggregate:
            return "Unable to create shareable link"

        if not self.query_identifier:
            return "Unable to create shareable link"

        if self._shareable_url:
            return self._shareable_url

        service = get_service(environment=self.region, tenant_id=self.tenant_id)

        result = service.sharelinks.mutation.create_share_link(
            ShareLinkCreateInput(
                link_ref=self.query_identifier,
                link_target="cql",
                link_type="queryId",
                tenant_id=self.tenant_id,
                extra_parameters=[
                    ExtraParamCreateInput(key="sourceType", value="alert"),
                ],
            )
        )

        self._shareable_url = (
            f'{service.core.sync_url.replace("api.", "")}/share/{result.id}'
        )
        return self._shareable_url


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

    list: Optional[List[CustomAlert2]] = field(
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
) -> Optional[AlertsResultsNormalizer]:
    """
    Search Taegis Alerts service.
    """
    service = get_service(environment=region, tenant_id=tenant)
    if not cell:
        cell = ""

    if "aggregate" in cell:
        limit = 1

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
                if sum(len(response.alerts.list) for response in poll_responses) >= int(
                    limit
                ):
                    break

    results = AlertsResultsNormalizer(
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
