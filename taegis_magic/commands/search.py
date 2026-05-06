"""Taegis Magic search commands."""

import inspect
import logging
from dataclasses import asdict
from typing import Optional

import typer
from taegis_magic.commands.utils.nl_queries import (
    clear_nl_search_queries,
    delete_nl_search_query,
    insert_nl_search_query,
    list_nl_search_queries,
)
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import (
    DataFrameNormalizer,
    TaegisResult,
    TaegisResults,
)
from taegis_magic.core.service import get_service
from typing_extensions import Annotated

from taegis_sdk_python.services.llm_service.types import (
    ExamplePromptCatalog,
    NLSearchFeedbackV2,
    NLSearchInputsV2,
    NLSearchRating,
)

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Search Commands.")


class NLSearchPromptCategories(TaegisResult):
    """Tenant Profiles single response normalizer."""

    raw_results: ExamplePromptCatalog

    @property
    def results(self):
        return [asdict(r) for r in self.raw_results.categories or []]


@app.command(name="generate")
@tracing
def nl_search_generate(
    cell: Annotated[
        str, typer.Option(help="Natural language query to convert to Taegis QL")
    ],
    limit: Annotated[
        Optional[int], typer.Option(help="Limit number of results")
    ] = None,
    database: Annotated[str, typer.Option()] = ":memory:",
    region: Annotated[Optional[str], typer.Option(help="Taegis Region")] = None,
):
    """Generate Taegis QL from natural language query."""
    service = get_service(environment=region)

    if limit:
        log.warning(
            "The 'limit' parameter is not currently supported by the Taegis Search API and will be ignored."
        )

    results = service.llm_service.query.nl_search_v2(
        in_=NLSearchInputsV2(
            query=cell,
        )
    )

    insert_nl_search_query(database, cell, results)

    return TaegisResult(
        raw_results=results,
        service="search",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments={
            "cell": cell,
            "limit": limit,
            "region": region,
        },
    )


@app.command(name="list")
@tracing
def nl_search_list(
    database: str = ":memory:",
):
    """List previous natural language search queries."""
    df = list_nl_search_queries(database)

    return DataFrameNormalizer(
        raw_results=df,
        service="search",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )


@app.command(name="delete")
@tracing
def nl_search_delete(
    query_id: Annotated[str, typer.Option()],
    database: str = ":memory:",
):
    """Remove a Taegis search query by id."""
    delete_nl_search_query(database, query_id)

    results = list_nl_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="clear")
@tracing
def nl_search_clear(
    database: Annotated[str, typer.Option()] = ":memory:",
):
    """Remove all Taegis search queries."""
    clear_nl_search_queries(database)

    results = list_nl_search_queries(database)

    normalized_results = DataFrameNormalizer(
        raw_results=results,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="feedback")
@tracing
def nl_search_feedback(
    rating: Annotated[
        NLSearchRating, typer.Argument(help="Rating of the generated query response.")
    ],
    query_id: Annotated[
        str,
        typer.Option(
            help="ID of the search query to provide feedback for.  @last for the most recent query."
        ),
    ] = "@last",
    comment: Annotated[
        Optional[str],
        typer.Option(help="Additional feedback about the generated query response."),
    ] = None,
    suggestion: Annotated[
        Optional[str],
        typer.Option(help="Suggested improvements to the generated query response."),
    ] = None,
    region: Annotated[Optional[str], typer.Option(help="Taegis Region")] = None,
):
    """Provide feedback about a Taegis search query."""

    service = get_service(environment=region)

    if query_id == "@last":
        df = list_nl_search_queries(":memory:")
        if df.empty:
            raise ValueError("No previous search queries found to provide feedback on.")
        query_id = df.sort_values(by="inserted_time", ascending=False).iloc[0].id

    results = service.llm_service.mutation.nl_search_feedback_v2(
        in_=NLSearchFeedbackV2(
            id=query_id,
            modified=suggestion,
            feedback_comment=comment,
            rating=rating,
        )
    )

    normalized_results = TaegisResult(
        raw_results=results,
        service="investigations",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="prompt-categories")
@tracing
def nl_search_prompt_categories(
    region: Annotated[Optional[str], typer.Option(help="Taegis Region")] = None,
):
    """List example prompt categories for Taegis Search."""
    service = get_service(environment=region)

    results = service.llm_service.query.example_prompt_catalog()

    return NLSearchPromptCategories(
        raw_results=results,
        service="search",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )


@app.command(name="prompts")
@tracing
def nl_search_prompts(
    region: Annotated[Optional[str], typer.Option(help="Taegis Region")] = None,
):
    """List example prompts for Taegis Search."""
    service = get_service(environment=region)

    results = service.llm_service.query.example_prompts()

    return TaegisResults(
        raw_results=results,
        service="search",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )
