"""Taegis Magic investigations commands."""
import inspect
import logging
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Union

import typer
from dataclasses_json import dataclass_json
from taegis_magic.commands.utils.investigations import (
    InvestigationEvidenceNormalizer,
    InvestigationEvidenceType,
    clear_search_queries,
    delete_search_query,
    find_database,
    find_dataframe,
    get_investigation_evidence,
    insert_search_query,
    list_search_queries,
    read_database,
    stage_investigation_evidence,
    unstage_investigation_evidence,
)
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import DataFrameNormalizer, TaegisResultsNormalizer

from taegis_magic.core.utils import remove_output_node
from taegis_magic.core.service import get_service
from taegis_magic.core.queries import get_query, update_query
from taegis_sdk_python import build_output_string
from taegis_sdk_python.services.investigations2.types import (
    CreateInvestigationInput,
    InvestigationStatus,
    InvestigationsV2,
    InvestigationsV2Arguments,
    InvestigationType,
    InvestigationV2,
)
from taegis_sdk_python.services.sharelinks.types import ShareLinkCreateInput
from typing_extensions import Annotated

log = logging.getLogger(__name__)


app = typer.Typer()
investigations_evidence = typer.Typer()
investigations_search_queries = typer.Typer()

app.add_typer(investigations_evidence, name="evidence")
app.add_typer(investigations_search_queries, name="search-queries")


class InvestigationPriority(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


INVESTIGATION_PRIORITY_MAP = {
    InvestigationPriority.LOW: 1,
    InvestigationPriority.MEDIUM: 2,
    InvestigationPriority.HIGH: 3,
    InvestigationPriority.CRITICAL: 4,
}


@dataclass_json
@dataclass
class InsertSearchQueryNormalizer:
    """Taegis Normalizer Query Normalizer (Duck Typed)."""

    query_identifier: str
    tenant_id: str
    query: str
    results_returned: int
    total_results: int


@dataclass_json
@dataclass
class InvestigationsSearchResultsNormalizer(TaegisResultsNormalizer):
    """Investigations Results Normalizer."""

    raw_results: Optional[List[InvestigationsV2]] = None
    _shareable_url: List[str] = field(default_factory=list)

    def __post_init__(self):
        self._shareable_url = [None for _ in range(self.results_returned)]

    @property
    def results(self) -> List[Dict[str, Any]]:
        """Query results from Investigations search.

        Returns
        -------
        List[Dict[str, Any]]
            List of results.
        """
        return (
            [
                asdict(investigation)
                for result in self.raw_results
                for investigation in result.investigations
            ]
            if self.raw_results
            else []
        )

    @property
    def status(self) -> str:
        """Status of query.

        Returns
        -------
        str
            Status message of query.
        """
        return "ERROR" if self.raw_results is None else "SUCCESS"

    @property
    def total_results(self) -> int:
        """Total number found by API.

        Returns
        -------
        int
            Total number found by API.
        """
        return -1 if self.raw_results is None else self.raw_results[0].total_count

    @property
    def results_returned(self) -> int:
        """Total number returned by API.

        Returns
        -------
        int
            Total number returned by API.
        """
        return -1 if self.raw_results is None else len(self.results)

    def get_shareable_url(self, index: int = 0) -> str:
        """Query Shareable Link.

        Returns
        -------
        str
            Returns a link created by the Preferences API.
        """
        if self.raw_results is None:
            return "No share link producible"

        investigation = self.results[index]

        if self._shareable_url[index]:
            return self._shareable_url[index]

        service = get_service(environment=self.region, tenant_id=self.tenant_id)

        result = service.sharelinks.mutation.create_share_link(
            ShareLinkCreateInput(
                link_ref=investigation.get("id"),
                link_type="investigationId",
                tenant_id=self.tenant_id,
            )
        )

        self._shareable_url[index] = (
            service.investigations.sync_url.replace("api.", "") + f"/share/{result.id}"
        )
        return self._shareable_url[index]


@dataclass_json
@dataclass
class InvestigationsCreatedResultsNormalizer(TaegisResultsNormalizer):
    """Investigations Results Normalizer."""

    raw_results: Union[InvestigationV2, CreateInvestigationInput] = field(
        default_factory=lambda: InvestigationV2()
    )
    dry_run: bool = False

    _shareable_url: Optional[str] = None

    @property
    def results(self):
        return [asdict(self.raw_results)]

    @property
    def status(self) -> str:
        """Status of query.

        Returns
        -------
        str
            Status message of query.
        """
        if self.dry_run:
            return "DRY_RUN"

        if self.raw_results:
            return "SUCCESS"

        return "ERROR"

    @property
    def total_results(self) -> int:
        """Total number found by API.

        Returns
        -------
        int
            Total number found by API.
        """
        return 0 if self.raw_results is None else 1

    @property
    def results_returned(self) -> int:
        """Total number returned by API.

        Returns
        -------
        int
            Total number returned by API.
        """
        return -1 if self.raw_results is None else len(self.results)

    def _repr_markdown_(self):
        if self.dry_run:
            return dedent(
                f"""
                Dry Run:

                ```json
                {self.raw_results.to_json()}
                ```
                """
            )
        else:
            return dedent(
                f"""
                | Investigation ID  | Short ID                | Title                | Type                | Share Link           |
                | ----------------- | ----------------------- | -------------------- | ------------------- | -------------------- |
                | {self.raw_results.id} | {self.raw_results.short_id} | {self.raw_results.title} | {self.raw_results.type} | {self.shareable_url} |
                """
            )

    @property
    def shareable_url(self) -> str:
        """Create a Shareable URL.

        Returns
        -------
        str
            Returns a link created by the Sharelinks API.
        """
        if self._shareable_url:
            return self._shareable_url

        if not isinstance(self.raw_results, InvestigationV2):
            return "Not Available"

        investigation = self.raw_results

        service = get_service(environment=self.region, tenant_id=self.tenant_id)

        result = service.sharelinks.mutation.create_share_link(
            ShareLinkCreateInput(
                link_ref=investigation.id,
                link_type="investigationId",
                tenant_id=self.tenant_id,
            )
        )

        self._shareable_url = (
            service.investigations.sync_url.replace("api.", "") + f"/share/{result.id}"
        )

        return self._shareable_url


@investigations_evidence.command(name="stage")
@tracing
def evidence_stage(
    evidence_type: InvestigationEvidenceType,
    dataframe: str,
    database: str = ":memory:",
    investigation_id: str = "NEW",
):
    """
    Stage evidence prior to linking to an investigation
    """
    df = find_dataframe(dataframe)
    db = find_database(database)
    changes = stage_investigation_evidence(df, db, evidence_type, investigation_id)

    return InvestigationEvidenceNormalizer(
        raw_results=changes,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )


@investigations_evidence.command(name="unstage")
@tracing
def evidence_unstage(
    evidence_type: InvestigationEvidenceType,
    dataframe: str,
    database: str = ":memory:",
    investigation_id: str = "NEW",
):
    """
    Remove staged evidence prior to linking to an investigation
    """

    df = find_dataframe(dataframe)
    db = find_database(database)
    changes = unstage_investigation_evidence(df, db, evidence_type, investigation_id)

    return InvestigationEvidenceNormalizer(
        raw_results=changes,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )


@investigations_evidence.command(name="show")
@tracing
def evidence_show(
    evidence_type: Optional[InvestigationEvidenceType] = None,
    database: str = ":memory:",
    investigation_id: Optional[str] = None,
    tenant: Optional[str] = None,
):
    """
    Show currently staged evidence
    """
    db = find_database(database)
    df = read_database(
        db,
        evidence_type=evidence_type,
        tenant_id=tenant,
        investigation_id=investigation_id,
    )

    return DataFrameNormalizer(
        raw_results=df,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )


@app.command()
@tracing
def create(
    title: str = typer.Option(),
    key_findings: Path = typer.Option(),
    priority: InvestigationPriority = InvestigationPriority.MEDIUM,
    type_: Annotated[
        InvestigationType, typer.Option("--type")
    ] = InvestigationType.SECURITY_INVESTIGATION,
    status: InvestigationStatus = InvestigationStatus.OPEN,
    assignee_id: str = "@customer",
    database: str = ":memory:",
    dry_run: bool = False,
    region: Optional[str] = None,
    tenant: Optional[str] = None,
):
    """
    Create a new investigation
    """
    service = get_service(environment=region, tenant_id=tenant)

    alerts = None
    events = None
    search_queries = None

    if database:
        evidence = get_investigation_evidence(database, service.tenant_id, "NEW")
        alerts = evidence.alerts
        events = evidence.events
        search_queries = evidence.search_queries

    # verify and save valid search queries
    if not dry_run:
        valid_search_queries = []
        for search_query in search_queries or []:
            query = get_query(service, search_query)
            if query.get("error"):
                log.error(f"Error finding search query: {search_query}")
                continue

            if query.get("id"):
                query_update = {
                    "name": query.get("description", f"{title} Query"),
                    "metadata": query.get("metadata", {}),
                }
                for metadata in query_update.get("metadata", []):
                    if metadata.get("id", "") == "isSaved":
                        metadata["value"] == "true"

                query = update_query(service, search_query, query_update)
                if query.get("error"):
                    log.error(
                        f"Error saving search query::{search_query}::{query.get('error')}"
                    )
                    continue

                valid_search_queries.append(search_query)
        search_queries = valid_search_queries or None

    create_investigation_input = CreateInvestigationInput(
        alerts=alerts,
        assignee_id=assignee_id,
        events=events,
        key_findings=key_findings.read_text(),
        priority=INVESTIGATION_PRIORITY_MAP[priority],
        search_queries=search_queries,
        status=status,
        title=title,
        type=type_,
    )

    if dry_run:
        created_investigation = None
    else:
        created_investigation = (
            service.investigations2.mutation.create_investigation_v2(
                input_=create_investigation_input
            )
        )

    results = InvestigationsCreatedResultsNormalizer(
        raw_results=create_investigation_input if dry_run else created_investigation,
        service="investigations",
        tenant_id=service.tenant_id,
        region=service.environment,
        dry_run=dry_run,
        arguments=inspect.currentframe().f_locals,
    )

    return results


@app.command()
@tracing
def search(
    cell: Optional[str] = None,
    # search_children_tenants: bool = False,
    limit: Optional[int] = None,
    region: Optional[str] = None,
    tenant: Optional[str] = None,
):
    """Taegis Investigations search."""
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 100

    results = []

    # fix for CX-99036
    pattern = r"\|\s*(head|tail)\s*([0-9]+)"
    match = re.search(pattern, cell)

    if not limit:
        if match and match.group(1) == "tail":  # pragma: no cover
            log.warning(
                "tail is not currently supported, it will be used at the limit..."
            )

        if match:
            limit = int(match.group(2))
    elif match:  # pragma: no cover
        log.warning(
            f"limit and {match.group(1)} both provided, only limit will be honored..."
        )

    cell = re.sub(pattern, "", cell)

    if limit and limit < per_page:
        per_page = limit
    # endfix

    # fix for CX-103490
    output = build_output_string(InvestigationsV2)

    output = remove_output_node(output, "metric")
    output = remove_output_node(output, "metrics")
    # endfix

    with service(output=output):
        investigations_results = service.investigations2.query.investigations_v2(
            InvestigationsV2Arguments(
                page=page,
                per_page=per_page,
                cql=cell,
                # search_children_tenants=search_children_tenants,
            )
        )

    results.append(investigations_results)

    # fix for CX-99036
    if not limit or investigations_results.total_count < limit:
        limit = investigations_results.total_count
    # endfix

    while (
        sum_results := sum(len(result.investigations) for result in results)
    ) < limit:
        page += 1

        # fix for CX-99036
        if (per_page * page) > limit:
            per_page = limit - sum_results
        # endfix

        with service(output=output):
            investigations_results = service.investigations2.query.investigations_v2(
                InvestigationsV2Arguments(
                    page=page,
                    per_page=per_page,
                    cql=cell,
                    # search_children_tenants=search_children_tenants,
                )
            )
        results.append(investigations_results)

    normalized_results = InvestigationsSearchResultsNormalizer(
        raw_results=results,
        service="investigations",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@investigations_search_queries.command(name="add")
@tracing
def investigations_search_queries_add(
    query_id: Annotated[str, typer.Option()],
    tenant_id: Annotated[str, typer.Option()],
    query: Annotated[str, typer.Option()],
    results_returned: Annotated[int, typer.Option()] = 0,
    total_results: Annotated[int, typer.Option()] = 0,
    database: Annotated[str, typer.Option()] = ":memory:",
):
    """Add a Taegis investigations search query"""
    normalized_results = InsertSearchQueryNormalizer(
        query_identifier=query_id,
        tenant_id=tenant_id,
        query=query,
        results_returned=results_returned,
        total_results=total_results,
    )

    insert_search_query(database, normalized_results)

    results = list_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@investigations_search_queries.command(name="remove")
@tracing
def investigations_search_queries_remove(
    query_id: Annotated[str, typer.Option()],
    database: Annotated[str, typer.Option()] = ":memory:",
):
    """Add a Taegis investigations search query"""
    delete_search_query(database, query_id)

    results = list_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@investigations_search_queries.command(name="clear")
@tracing
def investigations_search_queries_clear(
    database: Annotated[str, typer.Option()] = ":memory:",
):
    """Add a Taegis investigations search query"""
    clear_search_queries(database)

    results = list_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@investigations_search_queries.command(name="list")
@tracing
def investigations_search_queries_list(
    database: str = ":memory:",
):
    """List a Taegis investigations search query"""
    results = list_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@investigations_search_queries.command(name="stage")
@tracing
def investigations_search_queries_stage(
    database: Annotated[str, typer.Option()] = ":memory:",
    investigation_id: Annotated[str, typer.Option()] = "NEW",
):
    """Add a Taegis investigations search query"""
    results = list_search_queries(database)

    db = find_database(database)

    stage_investigation_evidence(
        results, db, InvestigationEvidenceType.Query, investigation_id
    )

    clear_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results
