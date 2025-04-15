"""Taegis Magic investigations commands."""

import inspect
import logging
import mimetypes
import re
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Union

import requests
import typer
from dataclasses_json import config, dataclass_json
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
    lookup_assignee_id,
    read_database,
    stage_investigation_evidence,
    unstage_investigation_evidence,
)
from taegis_magic.core.callbacks import verify_file
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import (
    DataFrameNormalizer,
    TaegisResult,
    TaegisResults,
    TaegisResultsNormalizer,
)
from taegis_magic.core.service import get_service
from taegis_magic.core.utils import remove_output_node
from typing_extensions import Annotated

from taegis_sdk_python import (
    GraphQLNoRowsInResultSetError,
    GraphQLService,
    build_output_string,
    prepare_input,
)
from taegis_sdk_python.services.investigations2.types import (
    AddCommentToInvestigationInput,
    AddEvidenceToInvestigationInput,
    ArchiveInvestigationInput,
    CreateInvestigationInput,
    DeleteInvestigationFileInput,
    InitInvestigationFileUploadInput,
    InvestigationFilesV2Arguments,
    InvestigationFileV2Arguments,
    InvestigationStatus,
    InvestigationsV2,
    InvestigationsV2Arguments,
    InvestigationType,
    InvestigationV2,
    InvestigationV2Arguments,
    UpdateInvestigationV2Input,
)
from taegis_sdk_python.services.queries.types import QLQueriesInput
from taegis_sdk_python.services.sharelinks.types import ShareLinkCreateInput
from taegis_sdk_python.services.subjects.types import Subject as FederatedSubject

log = logging.getLogger(__name__)


app = typer.Typer(help="Taegis Investigation Commands.")
investigations_attachment = typer.Typer(
    help="Investigation File Attachment commands.")
investigations_evidence = typer.Typer(help="Investigation Evidence commands.")
investigations_search_queries = typer.Typer(
    help="Investigation Search Query commands.")

app.add_typer(
    investigations_attachment,
    name="attachment",
)
app.add_typer(
    investigations_evidence,
    name="evidence",
)
app.add_typer(
    investigations_search_queries,
    name="search-queries",
)


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

        service = get_service(environment=self.region,
                              tenant_id=self.tenant_id)

        result = service.sharelinks.mutation.create_share_link(
            ShareLinkCreateInput(
                link_ref=investigation.get("id"),
                link_type="investigationId",
                tenant_id=self.tenant_id,
            )
        )

        self._shareable_url[index] = (
            service.investigations.sync_url.replace(
                "api.", "") + f"/share/{result.id}"
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

        service = get_service(environment=self.region,
                              tenant_id=self.tenant_id)

        result = service.sharelinks.mutation.create_share_link(
            ShareLinkCreateInput(
                link_ref=investigation.id,
                link_type="investigationId",
                tenant_id=self.tenant_id,
            )
        )

        self._shareable_url = (
            service.investigations.sync_url.replace(
                "api.", "") + f"/share/{result.id}"
        )

        return self._shareable_url


@dataclass_json
@dataclass(order=True, eq=True, frozen=True)
class TaegisMagicInvestigationV2(InvestigationV2):
    contributor_subjects: Optional[List[FederatedSubject]] = field(
        default=None, metadata=config(field_name="contributorSubjects")
    )
    assignee_subject: Optional[FederatedSubject] = field(
        default=None, metadata=config(field_name="assigneeSubject")
    )
    created_by_subject: Optional[FederatedSubject] = field(
        default=None, metadata=config(field_name="createdBySubject")
    )
    updated_by_subject: Optional[FederatedSubject] = field(
        default=None, metadata=config(field_name="updatedBySubject")
    )


@dataclass_json
@dataclass(order=True, eq=True, frozen=True)
class TaegisMagicInvestigationsV2(InvestigationsV2):
    investigations: List[TaegisMagicInvestigationV2] = field(
        default_factory=list, metadata=config(field_name="investigations")
    )


def federated_investigation_create(
    service: GraphQLService, input_: CreateInvestigationInput
) -> TaegisMagicInvestigationV2:
    """createInvestigationV2 creates new investigation with the provided arguments.."""
    endpoint = "createInvestigationV2"

    result = service.investigations2.execute_mutation(
        endpoint=endpoint,
        variables={
            "input": prepare_input(input_),
        },
        output=build_output_string(TaegisMagicInvestigationV2),
    )
    if result.get(endpoint) is not None:
        return TaegisMagicInvestigationV2.from_dict(result.get(endpoint))
    raise GraphQLNoRowsInResultSetError("for mutation createInvestigationV2")


def federated_investigations_search(
    service, arguments: InvestigationsV2Arguments
) -> TaegisMagicInvestigationsV2:
    """investigationsV2 returns a list of investigations matching the provided arguments."""
    endpoint = "investigationsV2"

    result = service.investigations2.execute_query(
        endpoint=endpoint,
        variables={
            "arguments": prepare_input(arguments),
        },
        output=build_output_string(TaegisMagicInvestigationsV2),
    )
    if result.get(endpoint) is not None:
        return TaegisMagicInvestigationsV2.from_dict(result.get(endpoint))
    raise GraphQLNoRowsInResultSetError("for query investigationsV2")


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
    changes = stage_investigation_evidence(
        df, db, evidence_type, investigation_id)

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
    evidence_type: Annotated[
        InvestigationEvidenceType, typer.Option(
            help="Investigation Evidence Type.")
    ],
    dataframe: Annotated[str, typer.Option(help="Data Reference.")],
    database: Annotated[
        str,
        typer.Option(
            help="Local database file.  Use :memory: for in-memory database."),
    ],
    investigation_id: Annotated[
        str, typer.Option(help="Investigation Identifier.")
    ] = "NEW",
):
    """
    Remove staged evidence prior to linking to an investigation.
    """

    df = find_dataframe(dataframe)
    db = find_database(database)
    changes = unstage_investigation_evidence(
        df, db, evidence_type, investigation_id)

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
    title: Annotated[str, typer.Option(help="Title for the Investigation.")],
    key_findings: Annotated[
        Path, typer.Option(help="Markdown file with key findings.")
    ],
    priority: Annotated[
        InvestigationPriority, typer.Option(help="Investigation Priority.")
    ] = InvestigationPriority.MEDIUM,
    type_: Annotated[
        InvestigationType, typer.Option("--type", help="Investigation Type.")
    ] = InvestigationType.SECURITY_INVESTIGATION,
    status: Annotated[
        InvestigationStatus, typer.Option(help="Investigation Status.")
    ] = InvestigationStatus.OPEN,
    assignee_id: Annotated[
        str,
        typer.Option(
            help="ID for Investigation assignment, may use @me, @partner or @tenant for quick reference."
        ),
    ] = "@tenant",
    database: Annotated[
        str,
        typer.Option(
            help="Investigation Evidence database location.  Can be a file path or ':memory:'."
        ),
    ] = ":memory:",
    dry_run: Annotated[
        bool,
        typer.Option(
            help="Setting to true only prints parameters.  API call is not submitted."
        ),
    ] = False,
    region: Annotated[Optional[str], typer.Option(
        help="Region Identifier.")] = None,
    tenant: Annotated[Optional[str], typer.Option(
        help="Tenant Context ID.")] = None,
):
    """
    Create a new Investigation.
    """
    service = get_service(environment=region, tenant_id=tenant)

    alerts = None
    events = None
    search_queries = None

    if database:
        evidence = get_investigation_evidence(
            database, service.tenant_id, "NEW")
        alerts = evidence.alerts
        events = evidence.events
        search_queries = evidence.search_queries

    # verify and save valid search queries
    if not dry_run:
        if search_queries:
            queries = service.queries.query.ql_queries(
                QLQueriesInput(rns=search_queries)
            )

            search_queries = [query.rn for query in queries.queries or []]
        else:
            search_queries = []

        assignee_id = lookup_assignee_id(service, assignee_id)

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
        created_investigation = federated_investigation_create(
            service=service, input_=create_investigation_input
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
                "tail is not currently supported, it will be used as the limit..."
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
    output = build_output_string(TaegisMagicInvestigationsV2)

    output = remove_output_node(output, "metric")
    output = remove_output_node(output, "metrics")
    # endfix

    with service(output=output):
        investigations_results = federated_investigations_search(
            service=service,
            arguments=InvestigationsV2Arguments(
                page=page,
                per_page=per_page,
                cql=cell,
            ),
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
            investigations_results = federated_investigations_search(
                service=service,
                arguments=InvestigationsV2Arguments(
                    page=page,
                    per_page=per_page,
                    cql=cell,
                ),
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


@app.command(name="merge")
@tracing
def investigations_merge(
    source: Annotated[str, typer.Option(help="Source Investigation ID.")],
    target: Annotated[str, typer.Option(help="Target Investigation ID.")],
    close_status: Annotated[
        InvestigationStatus,
        typer.Option(
            help="Investigation Status to set for the source investigation.",
        ),
    ] = InvestigationStatus.CLOSED_INFORMATIONAL,
    tenant: Annotated[Optional[str], typer.Option(
        help="Taegis Tenant ID.")] = None,
    region: Annotated[Optional[str], typer.Option(
        help="Taegis Region ID.")] = None,
):
    """Merge investigations evidence and close the source investigation."""
    service = get_service(environment=region, tenant_id=tenant)

    source_investigation = service.investigations2.query.investigation_v2(
        InvestigationV2Arguments(
            id=source,
        )
    )

    if not source_investigation:
        raise ValueError("Source Investigation not found.")

    if source_investigation.status not in (
        InvestigationStatus.OPEN,
        InvestigationStatus.DRAFT,
    ):
        raise ValueError(
            "Source Investigation status must be OPEN or DRAFT to merge.")

    if source_investigation.archived_at is not None:
        raise ValueError("Source Investigation must not be archived to merge.")

    if "Merged Investigation" in source_investigation.title:
        raise ValueError("Source Investigation has already been merged.")

    target_investigation = service.investigations2.query.investigation_v2(
        InvestigationV2Arguments(
            id=target,
        )
    )

    if not target_investigation:
        raise ValueError("Target Investigation not found.")

    if target_investigation.status not in (
        InvestigationStatus.OPEN,
        InvestigationStatus.DRAFT,
    ):
        raise ValueError(
            "Target Investigation status must be OPEN or DRAFT to merge.")

    if target_investigation.archived_at is not None:
        raise ValueError("Target Investigation must not be archived to merge.")

    result = service.sharelinks.mutation.create_share_link(
        ShareLinkCreateInput(
            link_ref=source_investigation.id,
            link_type="investigationId",
            tenant_id=tenant,
        )
    )

    source_shareable_url = (
        service.investigations.sync_url.replace(
            "api.", "") + f"/share/{result.id}"
    )

    result = service.sharelinks.mutation.create_share_link(
        ShareLinkCreateInput(
            link_ref=target_investigation.id,
            link_type="investigationId",
            tenant_id=tenant,
        )
    )

    target_shareable_url = (
        service.investigations.sync_url.replace(
            "api.", "") + f"/share/{result.id}"
    )

    results = service.investigations2.mutation.add_comment_to_investigation(
        AddCommentToInvestigationInput(
            investigation_id=source_investigation.id,
            comment=f"Investigation evidence moved into {target_investigation.id} ({target_shareable_url}).",
        )
    )
    log.debug("Add comment to investigation:", results)

    results = service.investigations2.mutation.add_comment_to_investigation(
        AddCommentToInvestigationInput(
            investigation_id=target_investigation.id,
            comment=f"Investigation evidence moved from {source_investigation.id} ({source_shareable_url}).",
        )
    )
    log.debug("Add comment to investigation:", results)

    results = service.investigations2.mutation.update_investigation_v2(
        UpdateInvestigationV2Input(
            id=source_investigation.id,
            status=close_status,
            title=f"Merged Investigation: {source_investigation.title}",
        )
    )
    log.debug("Close source investigation:", results)
    results = service.investigations2.mutation.archive_investigation_v2(
        ArchiveInvestigationInput(id=source_investigation.id)
    )
    log.debug("Archive source investigation:", results)

    results = service.investigations2.mutation.add_evidence_to_investigation(
        AddEvidenceToInvestigationInput(
            investigation_id=target_investigation.id,
            alerts=[
                evidence.alert_id for evidence in source_investigation.alerts_evidence
            ],
            events=[
                evidence.event_id for evidence in source_investigation.events_evidence
            ],
            search_queries=[
                evidence.search_query
                for evidence in source_investigation.search_queries_evidence
            ],
        )
    )
    log.debug("Add evidence to target investigation:", results)

    time.sleep(3)  # allow background workers to process
    target_investigation = service.investigations2.query.investigation_v2(
        InvestigationV2Arguments(
            id=target,
        )
    )

    normalized_results = InvestigationsCreatedResultsNormalizer(
        raw_results=target_investigation,
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
    """Add a Taegis investigations search query."""
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
    """Remove a Taegis investigations search query."""
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
    """Remove all Taegis investigations search queries."""
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
    """List tracked Taegis investigations search queries."""
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
    """Stage Taegis investigations search queries to attach to investigation."""
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


@investigations_attachment.command(name="list")
@tracing
def investigations_attachment_list(
    investigation_id: Annotated[str, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """List file attachments for a given investigation."""
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 20

    files = []

    results = service.investigations2.query.investigation_files_v2(
        InvestigationFilesV2Arguments(
            investigation_id=investigation_id,
            page=page,
            per_page=per_page,
        )
    )
    total_count = results.total_count
    files.extend(results.files)

    remaining_pages = -(-(total_count - per_page) // per_page)

    for page in range(2, remaining_pages + 2):
        results = service.investigations2.query.investigation_files_v2(
            InvestigationFilesV2Arguments(
                investigation_id=investigation_id,
                page=page,
                per_page=per_page,
            )
        )
        files.extend(results.files)

    normalized_results = TaegisResults(
        raw_results=files,
        service="investigations",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@investigations_attachment.command(name="get")
@tracing
def investigations_attachment_get(
    file_id: Annotated[str, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Get a file attachment."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.investigations2.query.investigation_file_v2(
        InvestigationFileV2Arguments(
            file_id=file_id,
        )
    )

    normalized_results = TaegisResult(
        raw_results=results,
        service="investigations",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@investigations_attachment.command(name="remove")
@tracing
def investigations_attachment_remove(
    file_id: Annotated[str, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Delete file attachment."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.investigations2.mutation.delete_investigation_file(
        DeleteInvestigationFileInput(
            file_id=file_id,
        )
    )

    normalized_results = TaegisResult(
        raw_results=results,
        service="investigations",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@investigations_attachment.command(name="upload")
@tracing
def investigations_attachment_upload(
    investigation_id: Annotated[str, typer.Option()],
    file: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            writable=False,
            readable=True,
            resolve_path=True,
        ),
    ],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Upload file attachment."""
    service = get_service(environment=region, tenant_id=tenant)

    file_input = InitInvestigationFileUploadInput(
        investigation_id=investigation_id,
        name=file.name,
        content_type=str(mimetypes.guess_type(file)[0]),
        size=file.stat().st_size,
    )
    log.debug(file_input)

    results = service.investigations2.mutation.init_investigation_file_upload(
        input_=file_input
    )
    log.debug(results)

    with file.open("rb") as f:
        upload_response = requests.put(
            results.presigned_url,
            headers={
                "Accept": "*/*",
                "Content-Type": str(mimetypes.guess_type(file)[0]),
                "Content-Length": str(file.stat().st_size),
            },
            data=f,
        )
    log.debug(upload_response)
    time.sleep(3)

    verify_upload = service.investigations2.query.investigation_file_v2(
        InvestigationFileV2Arguments(
            file_id=results.file.id,
        )
    )
    log.debug(verify_upload)

    normalized_results = TaegisResult(
        raw_results=verify_upload,
        service="investigations",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@investigations_attachment.command("download")
@tracing
def investigations_attachment_download(
    file_id: Annotated[
        str,
        typer.Option(),
    ],
    save_as: Annotated[Optional[str], typer.Option()] = None,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Get a file attachment."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.investigations2.query.investigation_file_v2(
        InvestigationFileV2Arguments(
            file_id=file_id,
        )
    )

    if not results.download_url:
        log.error("Cannot download file, no download url found.")
        raise typer.Exit(code=1)

    with requests.get(results.download_url, stream=True) as r:
        r.raise_for_status()

        if save_as:
            filename = save_as
        else:
            filename = results.name

        file_path = verify_file(filename)

        with file_path.open("wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    normalized_results = TaegisResult(
        raw_results=results,
        service="investigations",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results
