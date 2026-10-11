"""Taegis Magic events commands."""

import inspect
import logging
from dataclasses import asdict, dataclass, field
from functools import partial
from typing import Any, Dict, List, Optional

import typer
from dataclasses_json import dataclass_json
from taegis_magic.commands.configure import QUERIES_SECTION
from taegis_magic.commands.utils.investigations import insert_search_query
from taegis_magic.commands.utils.nl_queries import insert_nl_search_query
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResults, TaegisResultsNormalizer
from taegis_magic.core.service import get_service
from taegis_magic.core.time_chunks import render_time_chunked_queries
from typing_extensions import Annotated

from taegis_sdk_python.commons import execute_delimited_queries
from taegis_sdk_python.config import get_config
from taegis_sdk_python.services.events.types import (
    Event,
    EventQueryOptions,
    EventQueryResults,
)
from taegis_sdk_python.services.llm_service.types import NLSearchInputsV2
from taegis_sdk_python.services.rules.types import RuleEventType
from taegis_sdk_python.services.sharelinks.types import (
    ExtraParamCreateInput,
    ShareLinkCreateInput,
)

log = logging.getLogger(__name__)

CONFIG = get_config()
if not CONFIG.has_section(QUERIES_SECTION):
    CONFIG.add_section(QUERIES_SECTION)


app = typer.Typer(help="Taegis Events Commands.")


@dataclass_json
@dataclass
class TaegisEventNormalizer(TaegisResultsNormalizer):
    """Taegis Events Normalizer."""

    raw_results: List[Event] = field(default_factory=list)

    @property
    def results(self) -> List[Dict[str, Any]]:
        """Results from event by ids query.

        Returns
        -------
        List[Dict[str, Any]]
            List of Events
        """
        return [asdict(event) for event in self.raw_results]


@dataclass_json
@dataclass
class TaegisEventQueryNormalizer(TaegisResultsNormalizer):
    """Taegis Event Query Result Normalizer."""

    raw_results: List[EventQueryResults] = field(default_factory=list)
    query: str = ""
    is_saved: bool = False
    _query_id = None
    _shareable_url = None

    def _repr_markdown_(self):
        """Represent as markdown."""
        return self._display_template(
            "taegis_search_results.md.jinja"
        )  # pragma: no test

    @property
    def results(self) -> List[Dict[str, Any]]:
        """Results from Taegis event query.

        Returns
        -------
        List[Dict[str, Any]]
            List of Taegis event objects from query results.
        """

        rows = []
        for result in self.raw_results or []:
            if result.result:
                if result.result.rows:
                    rows.extend(result.result.rows)
                else:
                    log.info(f"No rows found in result: {result.result}")
            else:
                log.info(f"Non valid result set found: {result}")

        return rows

    @property
    def status(self) -> str:
        """Response status of Taegis event query.

        Returns
        -------
        str
            Command result status.
        """
        return self.raw_results[0].result.status if self.raw_results else "Error"

    @property
    def total_results(self) -> int:
        """Query results total number of results on server.

        This query type does not have a valid server return valid.

        Returns
        -------
        int
            Returns number of results.
        """
        # We cannot trust the result totals provided by the server.
        return -1

    @property
    def results_returned(self) -> int:
        """Query results total number of results returned.

        Returns
        -------
        str
            Returns number of results.
        """
        return len(self.results)

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
                    ExtraParamCreateInput(key="sourceType", value="event"),
                ],
            )
        )

        return f'{service.core.sync_url.replace("api.", "")}/share/{result.id_}'

    @property
    def shareable_url(self) -> str:
        """Generate a shareable url for Taegis XDR.

        Returns
        -------
        str
            Share Link url.
        """
        if not self.raw_results:
            return "Not able to create shareable link"

        if not self.query_identifier:
            return "Unable to create shareable link"

        if self._shareable_url:
            return self._shareable_url

        self._shareable_url = self._create_share_link(self.query_identifier)
        return self._shareable_url


@dataclass_json
@dataclass
class ChunkedTaegisEventQueryNormalizer(TaegisEventQueryNormalizer):
    """Taegis Event Query Result Normalizer for time chunked searches."""

    @property
    def chunk_query_identifiers(self) -> List[str]:
        """Query identifier of each time chunk.

        Each time chunk is submitted as its own query; the result pages of a
        chunk share that query's identifier.

        Returns
        -------
        List[str]
            Query Identifiers
        """
        return list(
            dict.fromkeys(
                result.query_id for result in self.raw_results if result.query_id
            )
        )

    @property
    def chunks(self) -> int:
        """Number of time chunks returning results.

        Returns
        -------
        int
            Returns number of time chunks.
        """
        return len(self.chunk_query_identifiers)

    @property
    def shareable_url(self) -> str:
        """Generate a shareable url for Taegis XDR.

        Returns
        -------
        str
            Share Link url.
        """
        if self._shareable_url:
            return self._shareable_url

        # each time chunk has its own query identifier and shareable link
        if self.chunks > 1:
            if not self.raw_results:
                return "Not able to create shareable link"

            urls = [
                self._create_share_link(query_id)
                for query_id in self.chunk_query_identifiers
            ]

            self._shareable_url = "\n".join(urls)
            return self._shareable_url

        return super().shareable_url


def get_next_page(events_results: List[EventQueryResults]) -> Optional[str]:
    """Retrieve events next page indicator."""
    try:
        # the next page could be found in any of the result pages,
        # but we cannot garuntee which result it will be found in
        return next(
            iter({result.next_ for result in events_results if result.next_ is not None})
        )
    except StopIteration:
        return None


def _event_query(
    cell: str,
    region: Optional[str] = None,
    tenant: Optional[str] = None,
) -> List[EventQueryResults]:
    """Execute an events query and page through the results."""
    service = get_service(tenant_id=tenant, environment=region)
    options = EventQueryOptions(
        timestamp_ascending=True,
        page_size=1000,
        max_rows=100000,
        aggregation_off=False,
    )

    results = []

    result = service.events.subscription.event_query(
        cell,
        options=options,
        metadata={
            "callerName": CONFIG[QUERIES_SECTION].get(
                "callername", fallback="Taegis Magic"
            ),
        },
    )
    results.extend(result)
    next_page = get_next_page(result)

    while next_page:
        result = service.events.subscription.event_page(next_page)
        results.extend(result)
        next_page = get_next_page(result)

    return results


def _event_query_time_chunked(
    queries: str,
    region: Optional[str],
    tenant: Optional[str],
) -> List[EventQueryResults]:
    """Execute `---` delimited time chunked events queries concurrently."""
    chunk_results, errors = execute_delimited_queries(
        queries,
        partial(_event_query, region=region, tenant=tenant),
        error_handling="partial",
    )

    for error in errors:
        log.error(
            f"Cannot retrieve results for time chunk::{error.item}::{error.error}"
        )

    return [result for chunk_result in chunk_results for result in chunk_result]


@app.command()
@tracing
def search(
    cell: Optional[str] = None,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
    save: bool = False,
    track: Annotated[bool, typer.Option()] = CONFIG[QUERIES_SECTION].getboolean(
        "track",
        fallback=False,
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
):
    """Taegis Events search.

    --time-window and --time-chunk must be set together to search a total
    duration as concurrent time chunked queries.  Durations are expressed as
    a number and a unit (s, m, h, d, w, mo, y).  The query cannot set its own
    EARLIEST/LATEST; they are appended, unless the query already contains the
    `EARLIEST='{{ window.earliest }}' LATEST='{{ window.latest }}'` placeholders.
    """
    if not cell:
        cell = ""

    if bool(time_window) != bool(time_chunk):
        raise ValueError("--time-window and --time-chunk must be set together.")

    if time_window and ai:
        raise ValueError("--ai cannot be used with time chunked searches.")

    service = get_service(tenant_id=tenant, environment=region)

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
            "from alert" in llm_results.ql.lower()
            or "from detection" in llm_results.ql.lower()
        ):
            raise ValueError(
                "LLM did not return a query targeting the alerts service. Cannot proceed with search."
            )

        insert_nl_search_query(database, cell, llm_results)

        cell = llm_results.ql

    arguments = {
        "cell": cell,
        "tenant": service.tenant_id,
        "region": service.environment,
        "save": save,
        "track": track,
        "database": database,
    }

    if time_window and time_chunk:
        normalizer = ChunkedTaegisEventQueryNormalizer
        query = render_time_chunked_queries(cell, time_window, time_chunk)
        log.debug(f"Time chunked queries::{query}")
        results = _event_query_time_chunked(query, region, tenant)
        arguments.update(
            {
                "time_window": time_window,
                "time_chunk": time_chunk,
            }
        )
    else:
        normalizer = TaegisEventQueryNormalizer
        query = cell
        results = _event_query(cell, region=region, tenant=tenant)

    normalized_results = normalizer(
        raw_results=results,
        service="events",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
        query=query,
        is_saved=save,
    )

    if track:
        insert_search_query(database, normalized_results)

    return normalized_results


@app.command()
@tracing
def events(
    resource_id: Annotated[Optional[List[str]], typer.Option()] = None,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Get events by resource id."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    results = service.events.query.events(ids=resource_id)

    normalized_results = TaegisEventNormalizer(
        raw_results=results,
        service="events",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@app.command()
@tracing
def schema(
    type_: Annotated[RuleEventType, typer.Option("--type", help="Event Schema Type")],
    tenant: Annotated[Optional[str], typer.Option(help="Tenant ID")] = None,
    region: Annotated[Optional[str], typer.Option(help="Taegis Region")] = None,
):
    """Query Event Schema fields."""

    service = get_service(environment=region, tenant_id=tenant)

    schema_fields = service.rules.query.filter_keys(RuleEventType(type_))

    @dataclass_json
    @dataclass
    class SchemaField:
        key: str

    schema_fields = [SchemaField(f) for f in schema_fields]

    return TaegisResults(
        raw_results=schema_fields,
        service="events",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments={"type": type_, "tenant": tenant, "region": region},
    )
