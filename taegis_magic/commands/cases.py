"""Taegis Magic investigations commands."""

import inspect
import logging
import mimetypes
import re
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from itertools import product
from pathlib import Path
from textwrap import dedent
from typing import Annotated, Any, Optional

import requests
import typer
from dataclasses_json import config, dataclass_json
from taegis_sdk_python import (
    GraphQLNoRowsInResultSetError,
    GraphQLService,
    build_output_string,
    prepare_input,
)
from taegis_sdk_python.services.investigations2.types import (
    AddCaseComment,
    AddEvidenceToCaseInput,
    Case,
    CaseArguments,
    CaseCommentsArguments,
    CaseFileArguments,
    CaseFilesArguments,
    Cases,
    CasesArguments,
    CasesPagination,
    CreateCaseInput,
    CreateKeyFindingsDocumentInput,
    DeleteCaseCommentInput,
    DeleteCaseFileInput,
    DocumentType,
    MergeCaseInput,
    OffsetPagination,
    RemoveEvidenceFromCaseInput,
    StartCaseFileUploadInput,
    UpdateCaseCommentInput,
)
from taegis_sdk_python.services.queries.types import QLQueriesInput
from taegis_sdk_python.services.sharelinks.types import ShareLinkCreateInput
from taegis_sdk_python.services.subjects.types import Subject as FederatedSubject

from taegis_magic.commands.utils.investigations import (
    InvestigationEvidenceChanges,
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

log = logging.getLogger(__name__)


app = typer.Typer(help="Taegis Case Commands.")
cases_attachment = typer.Typer(help="Case File Attachment commands.")
cases_comment = typer.Typer(help="Case Comment commands.")
cases_evidence = typer.Typer(help="Case Evidence commands.")
cases_search_queries = typer.Typer(help="Case Search Query commands.")

app.add_typer(
    cases_attachment,
    name="attachment",
)
app.add_typer(
    cases_comment,
    name="comment",
)
app.add_typer(
    cases_evidence,
    name="evidence",
)
app.add_typer(
    cases_search_queries,
    name="search-queries",
)


class CasePriority(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


CASE_PRIORITY_MAP = {
    CasePriority.LOW: 1,
    CasePriority.MEDIUM: 2,
    CasePriority.HIGH: 3,
    CasePriority.CRITICAL: 4,
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
class CasesSearchResultsNormalizer(TaegisResultsNormalizer):
    """Case search results normalizer."""

    raw_results: Optional[Any] = None
    _shareable_url: Optional[list[str]] = field(default_factory=list)

    def __post_init__(self):
        self._shareable_url = [None for _ in range(self.results_returned)]

    @property
    def results(self) -> list[dict[str, Any]]:
        """Query results from case search."""
        return (
            [
                asdict(case)
                for result in self.raw_results
                for case in (result.cases or [])
            ]
            if self.raw_results
            else []
        )

    @property
    def status(self) -> str:
        """Status of query."""
        return "ERROR" if self.raw_results is None else "SUCCESS"

    @property
    def total_results(self) -> int:
        """Total number found by API."""
        return -1 if self.raw_results is None else self.raw_results[0].total_count or 0

    @property
    def results_returned(self) -> int:
        """Total number returned by API."""
        return -1 if self.raw_results is None else len(self.results)

    def get_shareable_url(self, index: int = 0) -> str:
        """Query Shareable Link."""
        if self.raw_results is None:
            return "No share link producible"

        case = self.results[index]

        if self._shareable_url[index]:
            return self._shareable_url[index]

        service = get_service(environment=self.region, tenant_id=self.tenant_id)

        result = service.sharelinks.mutation.create_share_link(
            ShareLinkCreateInput(
                link_ref=case.get("id"),
                link_type="caseId",
                tenant_id=self.tenant_id,
            )
        )

        self._shareable_url[index] = (
            service.investigations.sync_url.replace("api.", "") + f"/share/{result.id_}"
        )
        return self._shareable_url[index]


@dataclass_json
@dataclass
class CasesCreatedResultsNormalizer(TaegisResultsNormalizer):
    """Case Results Normalizer."""

    raw_results: Any = field(default_factory=lambda: Case())
    dry_run: bool = False

    _shareable_url: Optional[str] = None

    @property
    def results(self):
        return [asdict(self.raw_results)]

    @property
    def status(self) -> str:
        """Status of query."""
        if self.dry_run:
            return "DRY_RUN"

        if self.raw_results:
            return "SUCCESS"

        return "ERROR"

    @property
    def total_results(self) -> int:
        """Total number found by API."""
        return 0 if self.raw_results is None else 1

    @property
    def results_returned(self) -> int:
        """Total number returned by API."""
        return -1 if self.raw_results is None else len(self.results)

    def _repr_markdown_(self):
        if self.dry_run:
            payload = (
                self.raw_results.to_json()
                if hasattr(self.raw_results, "to_json")
                else str(self.raw_results)
            )
            return dedent(
                f"""
                Dry Run:

                ```json
                {payload}
                ```
                """
            )
        else:
            case_id = getattr(self.raw_results, "id_", None)
            short_id = getattr(self.raw_results, "short_id", None)
            title = getattr(self.raw_results, "title", None)
            case_type = getattr(self.raw_results, "type_", None)
            return dedent(
                f"""
                | Case ID  | Short ID                | Title                | Type                | Share Link           |
                | -------- | ----------------------- | -------------------- | ------------------- | -------------------- |
                | {case_id} | {short_id} | {title} | {case_type} | {self.shareable_url} |
                """
            )

    @property
    def shareable_url(self) -> str:
        """Create a Shareable URL."""
        if self._shareable_url:
            return self._shareable_url

        if not isinstance(self.raw_results, Case):
            return "Not Available"

        case = self.raw_results

        service = get_service(environment=self.region, tenant_id=self.tenant_id)

        result = service.sharelinks.mutation.create_share_link(
            ShareLinkCreateInput(
                link_ref=case.id_,
                link_type="caseId",
                tenant_id=self.tenant_id,
            )
        )

        self._shareable_url = (
            service.investigations.sync_url.replace("api.", "") + f"/share/{result.id_}"
        )

        return self._shareable_url


@dataclass_json
@dataclass(order=True, eq=True, frozen=True)
class TaegisMagicCase(Case):
    contributor_subjects: Optional[list[FederatedSubject]] = field(
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
class TaegisMagicCases(Cases):
    cases: Optional[list[TaegisMagicCase]]  = field(
        default=None, metadata=config(field_name="cases")
    )


def build_create_case_input(
    *,
    title: str,
    key_findings: Path,
    severity: CasePriority,
    type_id: Optional[str],
    primary_status_id: Optional[str],
    assignee_id: str,
    alerts: Optional[list[str]],
    events: Optional[list[str]],
    search_queries: Optional[list[str]],
) -> CreateCaseInput:
    """Build a Taegis SDK CreateCaseInput from the CLI arguments."""
    return CreateCaseInput(
        title=title,
        severity=CASE_PRIORITY_MAP[severity],
        type_id=type_id,
        primary_status_id=primary_status_id,
        assignee_id=assignee_id,
        detection_ids=[alert for alert in (alerts or []) if alert],
        event_ids=[event for event in (events or []) if event],
        search_queries=[query for query in (search_queries or []) if query],
        key_findings=CreateKeyFindingsDocumentInput(
            content=key_findings.read_text(),
            document_type=DocumentType.MARKDOWN,
        ),
    )


def federated_case_create(
    service: GraphQLService, input_: CreateCaseInput
) -> TaegisMagicCase:
    """createCase creates a new case with the provided arguments."""
    endpoint = "createCase"

    result = service.investigations2.execute_mutation(
        endpoint=endpoint,
        variables={
            "input": prepare_input(input_),
        },
        output=build_output_string(TaegisMagicCase),
    )
    if result.get(endpoint) is not None:
        return TaegisMagicCase.from_dict(result.get(endpoint))
    raise GraphQLNoRowsInResultSetError("for mutation createCase")


def federated_cases_search(
    service, arguments: CasesArguments
) -> TaegisMagicCases:
    """cases returns a list of cases matching the provided arguments."""
    endpoint = "cases"

    result = service.investigations2.execute_query(
        endpoint=endpoint,
        variables={
            "arguments": prepare_input(arguments),
        },
        output=build_output_string(TaegisMagicCases),
    )
    if result.get(endpoint) is not None:
        return TaegisMagicCases.from_dict(result.get(endpoint))
    raise GraphQLNoRowsInResultSetError("for query cases")


@cases_evidence.command(name="stage")
@tracing
def evidence_stage(
    evidence_type: Annotated[
        Optional[InvestigationEvidenceType],
        typer.Argument(
            help="InvestigationType; will gather type from DataFrame if not provided."
        ),
    ],
    dataframe: Annotated[str, typer.Argument(help="Data Reference.")],
    database: Annotated[
        str, typer.Option(help="Database reference.  Provide a file path or :memory:")
    ] = ":memory:",
    case_id: Annotated[
        str, typer.Option(help="Taegis Case Identifier.")
    ] = "NEW",
):
    """
    Stage evidence prior to linking to a case.
    """
    arguments = inspect.currentframe().f_locals
    df = find_dataframe(dataframe)
    db = find_database(database)

    if evidence_type == evidence_type.All:
        if "taegis_magic.evidence_type" not in df.columns:
            raise ValueError(
                "DataFrame must contain 'taegis_magic.evidence_type' column to stage all evidence types."
            )

        changes = InvestigationEvidenceChanges(
            action="stage", evidence_type="All", investigation_id=case_id
        )

        for type_, id_ in product(
            df["taegis_magic.evidence_type"].unique(), df["case_id"].unique()
        ):
            stage_df = df[
                (
                    (df["taegis_magic.evidence_type"] == type_)
                    & (df["case_id"] == id_)
                )
            ]

            type_changes = stage_investigation_evidence(
                stage_df,
                db,
                type_,
                id_,
            )

            changes.before += type_changes.before
            changes.after += type_changes.after
            changes.difference += type_changes.difference
            log.debug(f"Staged evidence type ({type_}): {changes.to_json()}")
    else:
        changes = stage_investigation_evidence(df, db, evidence_type, case_id)

    return InvestigationEvidenceNormalizer(
        raw_results=changes,
        service="cases",
        tenant_id="N/A",
        region="N/A",
        arguments=arguments,
    )


@cases_evidence.command(name="unstage")
@tracing
def evidence_unstage(
    evidence_type: Annotated[
        InvestigationEvidenceType, typer.Argument(help="Investigation Evidence Type.")
    ],
    dataframe: Annotated[str, typer.Argument(help="Data Reference.")],
    database: Annotated[
        str,
        typer.Option(help="Local database file.  Use :memory: for in-memory database."),
    ] = ":memory:",
    case_id: Annotated[
        str, typer.Option(help="Case Identifier.")
    ] = "NEW",
):
    """
    Remove staged evidence prior to linking to a case.
    """

    arguments = inspect.currentframe().f_locals

    df = find_dataframe(dataframe)
    db = find_database(database)

    df["case_id"].fillna(case_id, inplace=True)

    if evidence_type == evidence_type.All:
        if "taegis_magic.evidence_type" not in df.columns:
            raise ValueError(
                "DataFrame must contain 'taegis_magic.evidence_type' column to unstage all evidence types."
            )

        changes = InvestigationEvidenceChanges(
            action="unstage", evidence_type="All", investigation_id=case_id
        )

        for type_, id_ in product(
            df["taegis_magic.evidence_type"].unique(), df["case_id"].unique()
        ):
            stage_df = df[
                (
                    (df["taegis_magic.evidence_type"] == type_)
                    & (df["case_id"] == id_)
                )
            ]

            type_changes = unstage_investigation_evidence(
                stage_df,
                db,
                type_,
                id_,
            )

            changes.before += type_changes.before
            changes.after += type_changes.after
            changes.difference += type_changes.difference
            log.debug(f"Staged evidence type ({type_}): {changes.to_json()}")
    else:
        changes = unstage_investigation_evidence(
            df, db, evidence_type, case_id
        )

    return InvestigationEvidenceNormalizer(
        raw_results=changes,
        service="cases",
        tenant_id="N/A",
        region="N/A",
        arguments=arguments,
    )


@cases_evidence.command(name="clear")
@tracing
def evidence_clear(
    database: Annotated[
        str,
        typer.Option(help="Local database file.  Use :memory: for in-memory database."),
    ] = ":memory:",
    case_id: Annotated[
        str, typer.Option(help="Case Identifier.")
    ] = "NEW",
    tenant: Annotated[
        Optional[str], typer.Option(help="Taegis Tenant Identifier.")
    ] = None,
):
    """
    Clear all currently staged evidence.
    """
    arguments = inspect.currentframe().f_locals
    db = find_database(database)

    for evidence_type in InvestigationEvidenceType:
        df = read_database(
            db,
            evidence_type=evidence_type,
            tenant_id=tenant,
            investigation_id=case_id,
        )
        log.debug(f"Found evidence type ({evidence_type}): {df.to_markdown()}")
        changes = unstage_investigation_evidence(
            df, db, evidence_type, case_id
        )
        log.debug(f"Unstaged evidence: {changes.to_json()}")

    df = read_database(
        db,
        evidence_type=None,
        tenant_id=tenant,
        investigation_id=case_id,
    )

    return DataFrameNormalizer(
        raw_results=df,
        service="cases",
        tenant_id="N/A",
        region="N/A",
        arguments=arguments,
    )


@cases_evidence.command(name="show")
@tracing
def evidence_show(
    evidence_type: Optional[InvestigationEvidenceType] = None,
    database: str = ":memory:",
    case_id: Optional[str] = None,
    tenant: Optional[str] = None,
):
    """
    Show currently staged evidence.
    """
    arguments = inspect.currentframe().f_locals
    db = find_database(database)
    df = read_database(
        db,
        evidence_type=evidence_type,
        tenant_id=tenant,
        investigation_id=case_id,
    )

    return DataFrameNormalizer(
        raw_results=df,
        service="cases",
        tenant_id="N/A",
        region="N/A",
        arguments=arguments,
    )


@app.command()
@tracing
def create(
    title: Annotated[str, typer.Option(help="Title for the Case.")],
    key_findings: Annotated[
        Path, typer.Option(help="Markdown file with key findings.")
    ],
    priority: Annotated[
        CasePriority, typer.Option(help="Case Priority.")
    ] = CasePriority.MEDIUM,
    type_id: Annotated[
        Optional[str], typer.Option("--type-id", help="Case Type Identifier.")
    ] = None,
    primary_status_id: Annotated[
        Optional[str], typer.Option(help="Case Primary Status Identifier.")
    ] = None,
    assignee_id: Annotated[
        str,
        typer.Option(
            help="ID for case assignment, may use @me, @partner or @tenant for quick reference."
        ),
    ] = "@tenant",
    database: Annotated[
        str,
        typer.Option(
            help="Investigation Evidence database location. Can be a file path or ':memory:'."
        ),
    ] = ":memory:",
    dry_run: Annotated[
        bool,
        typer.Option(
            help="Setting to true only prints parameters. API call is not submitted."
        ),
    ] = False,
    region: Annotated[Optional[str], typer.Option(help="Region Identifier.")] = None,
    tenant: Annotated[Optional[str], typer.Option(help="Tenant Context ID.")] = None,
):
    """Create a new case."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    alerts = None
    events = None
    search_queries = None

    if database:
        evidence = get_investigation_evidence(database, service.tenant_id, "NEW")
        alerts = evidence.alerts
        events = evidence.events
        search_queries = evidence.search_queries

    if not dry_run:
        if search_queries:
            queries = service.queries.query.ql_queries(
                QLQueriesInput(rns=search_queries)
            )
            search_queries = [query.rn for query in queries.queries or []]
        else:
            search_queries = []

        assignee_id = lookup_assignee_id(service, assignee_id)

    create_case_input = build_create_case_input(
        title=title,
        key_findings=key_findings,
        severity=priority,
        type_id=type_id,
        primary_status_id=primary_status_id,
        assignee_id=assignee_id,
        alerts=alerts,
        events=events,
        search_queries=search_queries,
    )

    if dry_run:
        created_case = None
    else:
        created_case = federated_case_create(service=service, input_=create_case_input)

    results = CasesCreatedResultsNormalizer(
        raw_results=create_case_input if dry_run else created_case,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        dry_run=dry_run,
        arguments=arguments,
    )

    return results


@cases_evidence.command(name="append")
@tracing
def evidence_append(
    case_id: Annotated[str, typer.Option(help="Case Identifier.")],
    database: Annotated[
        str,
        typer.Option(
            help="Investigation Evidence database location. Can be a file path or ':memory:'."
        ),
    ] = ":memory:",
    use_new: Annotated[
        bool, typer.Option(help="Use NEW case id for evidence.")
    ] = False,
    region: Annotated[Optional[str], typer.Option(help="Region Identifier.")] = None,
    tenant: Annotated[Optional[str], typer.Option(help="Tenant Context ID.")] = None,
):
    """Append evidence to an existing case."""
    arguments = inspect.currentframe().f_locals
    if not database:
        raise ValueError("Database must be provided to append evidence.")

    service = get_service(environment=region, tenant_id=tenant)

    alerts = None
    events = None
    search_queries = None

    evidence = get_investigation_evidence(
        database, service.tenant_id, "NEW" if use_new else case_id
    )
    alerts = evidence.alerts
    events = evidence.events
    search_queries = evidence.search_queries

    results = service.investigations2.mutation.add_evidence_to_case(
        AddEvidenceToCaseInput(
            case_id=case_id,
            detection_ids=alerts,
            event_ids=events,
            search_queries=search_queries,
        )
    )
    log.debug(f"Add evidence API results: {results}")

    results = service.investigations2.query.case(
        CaseArguments(
            id_=case_id,
        )
    )

    return CasesCreatedResultsNormalizer(
        raw_results=results,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )


@cases_evidence.command(name="remove")
@tracing
def evidence_remove(
    case_id: Annotated[str, typer.Option(help="Case Identifier.")],
    database: Annotated[
        str,
        typer.Option(
            help="Case Evidence database location. Can be a file path or ':memory:'."
        ),
    ] = ":memory:",
    use_new: Annotated[
        bool, typer.Option(help="Use NEW case id for evidence.")
    ] = False,
    region: Annotated[Optional[str], typer.Option(help="Region Identifier.")] = None,
    tenant: Annotated[Optional[str], typer.Option(help="Tenant Context ID.")] = None,
):
    """Remove evidence from an existing case."""
    arguments = inspect.currentframe().f_locals
    if not database:
        raise ValueError("Database must be provided to remove evidence.")

    service = get_service(environment=region, tenant_id=tenant)

    alerts = None
    events = None
    search_queries = None

    evidence = get_investigation_evidence(
        database, service.tenant_id, "NEW" if use_new else case_id
    )
    log.debug(f"Retrieved evidence for case: {evidence}")
    alerts = evidence.alerts
    events = evidence.events
    search_queries = evidence.search_queries

    results = service.investigations2.mutation.remove_evidence_from_case(
        RemoveEvidenceFromCaseInput(
            case_id=case_id,
            detection_ids=alerts,
            event_ids=events,
            search_queries=search_queries,
        )
    )
    log.debug(f"Remove evidence API results: {results}")

    results = service.investigations2.query.case(
        CaseArguments(
            id_=case_id,
        )
    )

    return CasesCreatedResultsNormalizer(
        raw_results=results,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )


@app.command()
@tracing
def search(
    cell: Optional[str] = None,
    limit: Optional[int] = None,
    region: Optional[str] = None,
    tenant: Optional[str] = None,
):
    """Taegis case search."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 100

    results = []

    pattern = r"\|\s*(head|tail)\s*([0-9]+)"
    match = re.search(pattern, cell or "")

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

    cell = re.sub(pattern, "", cell or "")

    if limit and limit < per_page:
        per_page = limit

    output = build_output_string(TaegisMagicCases)
    output = remove_output_node(output, "allowedNextTypes")
    output = remove_output_node(output, "allowedNextSources")

    with service(output=output):
        cases_results = federated_cases_search(
            service=service,
            arguments=CasesArguments(
                query=cell,
                pagination=CasesPagination(
                    offset=OffsetPagination(page=page, per_page=per_page)
                ),
            ),
        )

    results.append(cases_results)

    if not limit or cases_results.total_count < limit:
        limit = cases_results.total_count or 0

    while (sum_results := sum(len(result.cases or []) for result in results)) < limit:
        page += 1

        if (per_page * page) > limit:
            per_page = limit - sum_results

        with service(output=output):
            cases_results = federated_cases_search(
                service=service,
                arguments=CasesArguments(
                    query=cell,
                    pagination=CasesPagination(
                        offset=OffsetPagination(page=page, per_page=per_page)
                    ),
                ),
            )
        results.append(cases_results)

    normalized_results = CasesSearchResultsNormalizer(
        raw_results=results,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results





@app.command(name="merge")
@tracing
def cases_merge(
    source: Annotated[str, typer.Option(help="Source Case ID.")],
    target: Annotated[str, typer.Option(help="Target Case ID.")],
    tenant: Annotated[Optional[str], typer.Option(help="Taegis Tenant ID.")] = None,
    region: Annotated[Optional[str], typer.Option(help="Taegis Region ID.")] = None,
):
    """Merge evidence from one case into another."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    source_case = service.investigations2.query.case(
        CaseArguments(
            id_=source,
        )
    )

    if not source_case:
        raise ValueError("Source case not found.")

    if not source_case.id_:
        raise ValueError("Source case id is missing.")

    if source_case.archived_at is not None:
        raise ValueError("Source case must not be archived to merge.")

    target_case = service.investigations2.query.case(
        CaseArguments(
            id_=target,
        )
    )

    if not target_case:
        raise ValueError("Target case not found.")

    if not target_case.id_:
        raise ValueError("Target case id is missing.")

    if target_case.archived_at is not None:
        raise ValueError("Target case must not be archived to merge.")

    result = service.investigations2.mutation.merge_case(
        MergeCaseInput(
            target_case_id=target_case.id_,
            source_case_ids=[source_case.id_],
            archive_sources=True,
        )
    )
    log.debug(f"Merge case API results: {result}")

    time.sleep(3)
    target_case = service.investigations2.query.case(
        CaseArguments(
            id_=target,
        )
    )

    normalized_results = CasesCreatedResultsNormalizer(
        raw_results=target_case,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@cases_search_queries.command(name="add")
@tracing
def cases_search_queries_add(
    query_id: Annotated[Optional[str], typer.Option()],
    tenant_id: Annotated[Optional[str], typer.Option()],
    query: Annotated[Optional[str], typer.Option()],
    results_returned: Annotated[Optional[int], typer.Option()] = 0,
    total_results: Annotated[Optional[int], typer.Option()] = 0,
    database: Annotated[Optional[str], typer.Option()] = ":memory:",
):
    """Add a Taegis investigations search query."""
    arguments = inspect.currentframe().f_locals
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
        service="cases",
        tenant_id="N/A",
        region="N/A",
        arguments=arguments,
    )

    return normalized_results


@cases_search_queries.command(name="remove")
@tracing
def cases_search_queries_remove(
    query_id: Annotated[str, typer.Option()],
    database: Annotated[str, typer.Option()] = ":memory:",
):
    """Remove a Taegis investigations search query."""
    arguments = inspect.currentframe().f_locals
    delete_search_query(database, query_id)

    results = list_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="cases",
        tenant_id="N/A",
        region="N/A",
        arguments=arguments,
    )

    return normalized_results


@cases_search_queries.command(name="clear")
@tracing
def cases_search_queries_clear(
    database: Annotated[str, typer.Option()] = ":memory:",
):
    """Remove all Taegis investigations search queries."""
    arguments = inspect.currentframe().f_locals
    clear_search_queries(database)

    results = list_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="cases",
        tenant_id="N/A",
        region="N/A",
        arguments=arguments,
    )

    return normalized_results


@cases_search_queries.command(name="list")
@tracing
def cases_search_queries_list(
    database: str = ":memory:",
):
    """List tracked Taegis investigations search queries."""
    arguments = inspect.currentframe().f_locals
    results = list_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="cases",
        tenant_id="N/A",
        region="N/A",
        arguments=arguments,
    )

    return normalized_results


@cases_search_queries.command(name="stage")
@tracing
def cases_search_queries_stage(
    database: Annotated[str, typer.Option()] = ":memory:",
    case_id: Annotated[str, typer.Option()] = "NEW",
):
    """Stage Taegis case search queries to attach to a case."""
    arguments = inspect.currentframe().f_locals
    results = list_search_queries(database)

    db = find_database(database)

    stage_investigation_evidence(
        results, db, InvestigationEvidenceType.Query, case_id
    )

    clear_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="cases",
        tenant_id="N/A",
        region="N/A",
        arguments=arguments,
    )

    return normalized_results


@cases_attachment.command(name="list")
@tracing
def cases_attachment_list(
    case_id: Annotated[str, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """List file attachments for a given case."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 20

    files = []

    results = service.investigations2.query.case_files(
        CaseFilesArguments(
            query=f"caseId:{case_id}",
            page=page,
            per_page=per_page,
        )
    )
    total_count = results.total_count or 0
    files.extend(results.files or [])

    remaining_pages = -(-(total_count - per_page) // per_page)

    for page in range(2, remaining_pages + 2):
        results = service.investigations2.query.case_files(
            CaseFilesArguments(
                query=f"caseId:{case_id}",
                page=page,
                per_page=per_page,
            )
        )
        files.extend(results.files or [])

    normalized_results = TaegisResults(
        raw_results=files,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@cases_attachment.command(name="get")
@tracing
def cases_attachment_get(
    file_id: Annotated[str, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Get a file attachment."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    results = service.investigations2.query.case_file(
        CaseFileArguments(
            file_id=file_id,
        )
    )

    normalized_results = TaegisResult(
        raw_results=results,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@cases_attachment.command(name="remove")
@tracing
def cases_attachment_remove(
    file_id: Annotated[str, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Delete file attachment."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    results = service.investigations2.mutation.delete_case_file(
        DeleteCaseFileInput(
            file_id=file_id,
        )
    )

    normalized_results = TaegisResult(
        raw_results=results,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@cases_attachment.command(name="upload")
@tracing
def cases_attachment_upload(
    case_id: Annotated[str, typer.Option()],
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
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    file_input = StartCaseFileUploadInput(
        case_id=case_id,
        name=file.name,
        content_type=str(mimetypes.guess_type(file)[0]),
        size=file.stat().st_size,
    )
    log.debug(file_input)

    results = service.investigations2.mutation.start_case_file_upload(
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

    verify_upload = service.investigations2.query.case_file(
        CaseFileArguments(
            file_id=results.file.id_,
        )
    )
    log.debug(verify_upload)

    normalized_results = TaegisResult(
        raw_results=verify_upload,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@cases_attachment.command("download")
@tracing
def cases_attachment_download(
    file_id: Annotated[
        str,
        typer.Option(),
    ],
    save_as: Annotated[Optional[str], typer.Option()] = None,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Get a file attachment."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    results = service.investigations2.query.case_file(
        CaseFileArguments(
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
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@cases_comment.command(name="list")
@tracing
def cases_comments_list(
    case_id: Annotated[str, typer.Option(help="Case Identifier.")],
    tenant: Annotated[Optional[str], typer.Option(help="Tenant Identifier.")] = None,
    region: Annotated[Optional[str], typer.Option(help="Region Identifier.")] = None,
):
    """List comments for a case."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    page = 1

    results = service.investigations2.query.case_comments(
        CaseCommentsArguments(
            case_id=case_id,
            page=page,
            per_page=100,
        )
    )

    comments = results.comments or []
    total_count = results.total_count or 0

    while len(comments) < total_count:
        page += 1
        results = service.investigations2.query.case_comments(
            CaseCommentsArguments(
                case_id=case_id,
                page=page,
                per_page=100,
            )
        )

        comments.extend(results.comments or [])

    normalized_results = TaegisResults(
        raw_results=comments,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@cases_comment.command(name="add")
@tracing
def cases_comments_add(
    case_id: Annotated[str, typer.Option(help="Case Identifier.")],
    cell: Annotated[str, typer.Option(help="Comment text.")],
    is_internal: Annotated[
        bool, typer.Option(help="Mark comment as internal. For partner use only.")
    ] = False,
    mention: Annotated[
        Optional[list[str]],
        typer.Option(
            help="Mention a user, may use @me, @partner or @tenant for quick reference.  May be used multiple times."
        ),
    ] = None,
    tenant: Annotated[Optional[str], typer.Option(help="Tenant Identifier.")] = None,
    region: Annotated[Optional[str], typer.Option(help="Region Identifier.")] = None,
):
    """Add a comment to a case."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    for m in mention or []:
        if m.startswith("@"):
            m = lookup_assignee_id(service, m)
        cell += f"\n\n@{m}"

    results = service.investigations2.mutation.add_case_comment(
        AddCaseComment(
            case_id=case_id,
            comment=cell,
            is_internal=is_internal,
        )
    )

    normalized_results = TaegisResult(
        raw_results=results,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@cases_comment.command(name="update")
@tracing
def cases_comments_update(
    comment_id: Annotated[str, typer.Option(help="Comment Identifier.")],
    cell: Annotated[str, typer.Option(help="Comment text.")],
    mark_as_read: Annotated[
        bool, typer.Option(help="Mark comment as read. For partner use only.")
    ] = False,
    mention: Annotated[
        Optional[list[str]],
        typer.Option(
            help="Mention a user, may use @me, @partner or @tenant for quick reference.  May be used multiple times."
        ),
    ] = None,
    tenant: Annotated[Optional[str], typer.Option(help="Tenant Identifier.")] = None,
    region: Annotated[Optional[str], typer.Option(help="Region Identifier.")] = None,
):
    """Update a comment on a case."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    for m in mention or []:
        if m.startswith("@"):
            m = lookup_assignee_id(service, m)
        cell += f"\n{m}"

    results = service.investigations2.mutation.update_case_comment(
        UpdateCaseCommentInput(
            comment_id=comment_id,
            comment=cell,
            mark_as_read=mark_as_read,
        )
    )

    normalized_results = TaegisResult(
        raw_results=results,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results


@cases_comment.command(name="remove")
@tracing
def cases_comments_remove(
    comment_id: Annotated[str, typer.Option(help="Comment Identifier.")],
    tenant: Annotated[Optional[str], typer.Option(help="Tenant Identifier.")] = None,
    region: Annotated[Optional[str], typer.Option(help="Region Identifier.")] = None,
):
    """Remove a comment from a case."""
    arguments = inspect.currentframe().f_locals
    service = get_service(environment=region, tenant_id=tenant)

    results = service.investigations2.mutation.delete_case_comment(
        DeleteCaseCommentInput(
            comment_id=comment_id,
        )
    )

    normalized_results = TaegisResult(
        raw_results=results,
        service="cases",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=arguments,
    )

    return normalized_results
