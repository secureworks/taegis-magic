"""Taegis Magic tenants commands."""

import inspect
import logging
from dataclasses import asdict, dataclass, field
from typing import List, Optional

import typer
from dataclasses_json import dataclass_json
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.service import get_service
from taegis_magic.core.utils import remove_output_node
from taegis_sdk_python import (
    GraphQLNoRowsInResultSetError,
    build_output_string,
)
from taegis_sdk_python.services.rules.types import (
    Rule,
    RuleEventType,
    RuleQueryKind,
    RuleType,
    SearchRulesInput,
    SearchRulesOutput,
    RuleInput,
    RuleQLFilterInput,
    RuleFilterInput,
)
from typing_extensions import Annotated
from enum import Enum
from click.exceptions import BadOptionUsage


log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Rules Commands.")
create_rule = typer.Typer(help="Create a Custom Alerting or Suppression Rules.")
app.add_typer(create_rule, name="create")


class RuleSeverity(str, Enum):
    """Rule Severity Options."""

    low = "low"
    medium = "medium"
    high = "high"
    critical = "critical"


RULE_SEVERTIY_MAP = {
    "low": 0.2,
    "medium": 0.4,
    "high": 0.6,
    "critical": 0.8,
}


class RuleSuppressionKey(str, Enum):
    """Suppression Rule keys."""

    alert_title = "alert_title"
    detector = "detector"
    entity = "entity"


RULE_SUPPRESSION_KEY_MAP = {
    "alert_title": "scwx.observation_v2.title",
    "detector": "scwx.observation_v2.detector_id",
    "entity": "scwx.observation_v2.entity",
}


def multiple_rules_output():
    return remove_output_node(
        build_output_string(Rule),
        "generativeAIRuleExplain",
    )


@dataclass_json
@dataclass
class TaegisRuleNormalizer(TaegisResultsNormalizer):
    """Taegis Single Rule normalizer."""

    raw_results: Rule = field(default_factory=lambda: Rule())

    @property
    def results(self):
        return [asdict(self.raw_results)]


@dataclass
class TaegisRulesNormalizer(TaegisResultsNormalizer):
    """Taegis Multiple Rule normalizer."""

    raw_results: List[Rule] = field(default_factory=list)

    @property
    def results(self):
        return [asdict(rule) for rule in self.raw_results]


@dataclass
class TaegisRulesSearchNormalizer(TaegisResultsNormalizer):
    """Taegis Search Rule normalizer."""

    raw_results: SearchRulesOutput = field(default_factory=lambda: SearchRulesOutput())

    @property
    def results(self):
        return [asdict(rule) for rule in self.raw_results.rules]


@app.command(name="type")
@tracing
def rules_type(
    rule_type: Annotated[RuleType, typer.Option()] = RuleType.QL,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis rules by type."""
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 500
    rules = []

    log.info(f"Getting page: {page}")
    try:
        with service(output=multiple_rules_output()):
            results = service.rules.query.rules(
                page=page, count=per_page, rule_type=rule_type
            )
    except GraphQLNoRowsInResultSetError:  # pragma: no cover
        results = []
    log.debug(f"Results returned: {len(results)}")
    rules.extend(results)

    while len(results) == per_page:
        page += 1
        log.info(f"Getting page: {page}")
        try:
            with service(output=multiple_rules_output()):
                results = service.rules.query.rules(
                    page=page, count=per_page, rule_type=rule_type
                )
        except GraphQLNoRowsInResultSetError:  # pragma: no cover
            results = []
        log.debug(f"Results returned: {len(results)}")
        rules.extend(results)

    normalized_results = TaegisRulesNormalizer(
        raw_results=rules,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="all")
@tracing
def rules_all(
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis all rules."""
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 500
    rules = []

    log.info(f"Getting page: {page}")
    try:
        with service(output=multiple_rules_output()):
            results = service.rules.query.all_rules(page=page, count=per_page)
    except GraphQLNoRowsInResultSetError:  # pragma: no cover
        results = []
    log.debug(f"Results returned: {len(results)}")
    rules.extend(results)

    while len(results) == per_page:
        page += 1
        log.info(f"Getting page: {page}")
        try:
            with service(output=multiple_rules_output()):
                results = service.rules.query.all_rules(page=page, count=per_page)
        except GraphQLNoRowsInResultSetError:  # pragma: no cover
            results = []
        log.debug(f"Results returned: {len(results)}")
        rules.extend(results)

    normalized_results = TaegisRulesNormalizer(
        raw_results=rules,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="suppression")
@tracing
def rules_suppression(
    kind: RuleQueryKind = RuleQueryKind.ALL,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis suppression rules."""
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 500
    rules = []

    log.info(f"Getting page: {page}")
    try:
        results = service.rules.query.suppression_rules(
            page=page,
            count=per_page,
            kind=kind,
        )
    except GraphQLNoRowsInResultSetError:  # pragma: no cover
        results = []
    log.debug(f"Results returned: {len(results)}")
    rules.extend(results)

    while len(results) == per_page:
        page += 1
        log.info(f"Getting page: {page}")
        try:
            results = service.rules.query.suppression_rules(
                page=page,
                count=per_page,
                kind=kind,
            )
        except GraphQLNoRowsInResultSetError:  # pragma: no cover
            results = []
        log.debug(f"Results returned: {len(results)}")
        rules.extend(results)

    normalized_results = TaegisRulesNormalizer(
        raw_results=rules,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="deleted")
@tracing
def rules_deleted(
    rule_type: Annotated[RuleType, typer.Option()] = RuleType.QL,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis deleted rules."""
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 500
    rules = []

    log.info(f"Getting page: {page}")
    try:
        with service(output=multiple_rules_output()):
            results = service.rules.query.deleted_rules(
                page=page,
                count=per_page,
                rule_type=rule_type,
            )
    except GraphQLNoRowsInResultSetError:  # pragma: no cover
        results = []
    log.debug(f"Results returned: {len(results)}")
    rules.extend(results)

    while len(results) == per_page:
        page += 1
        log.info(f"Getting page: {page}")
        try:
            with service(output=multiple_rules_output()):
                results = service.rules.query.deleted_rules(
                    page=page,
                    count=per_page,
                    rule_type=rule_type,
                )
        except GraphQLNoRowsInResultSetError:  # pragma: no cover
            results = []
        log.debug(f"Results returned: {len(results)}")
        rules.extend(results)

    normalized_results = TaegisRulesNormalizer(
        raw_results=rules,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="filters")
@tracing
def rules_event_type(
    event_type: Annotated[RuleEventType, typer.Option()],
    rule_type: Annotated[RuleType, typer.Option()] = RuleType.REGEX,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis all rules."""
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 500
    rules = []

    log.info(f"Getting page: {page}")
    try:
        with service(output=multiple_rules_output()):
            results = service.rules.query.rules_for_event(
                page=page, count=per_page, event_type=event_type, rule_type=rule_type
            )
    except GraphQLNoRowsInResultSetError:  # pragma: no cover
        results = []
    log.debug(f"Results returned: {len(results)}")
    rules.extend(results)

    while len(results) == per_page:
        page += 1
        log.info(f"Getting page: {page}")
        try:
            with service(output=multiple_rules_output()):
                results = service.rules.query.rules_for_event(
                    page=page,
                    count=per_page,
                    event_type=event_type,
                    rule_type=rule_type,
                )
        except GraphQLNoRowsInResultSetError:  # pragma: no cover
            results = []
        log.debug(f"Results returned: {len(results)}")
        rules.extend(results)

    normalized_results = TaegisRulesNormalizer(
        raw_results=rules,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="rule")
@tracing
def rules_rule(
    rule_id: Annotated[str, typer.Option()],
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis rule by id."""
    service = get_service(environment=region, tenant_id=tenant)

    try:
        results = service.rules.query.rule(id_=rule_id)
    except GraphQLNoRowsInResultSetError:  # pragma: no cover
        results = Rule()

    normalized_results = TaegisRuleNormalizer(
        raw_results=results,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command("changes-since")
@tracing
def rules_changes_since(
    timestamp: Annotated[str, typer.Option(help="YYYY-DD-MMTHH:MM:SSZ")],
    event_type: Annotated[Optional[RuleEventType], typer.Option()] = None,
    rule_type: Annotated[RuleType, typer.Option()] = RuleType.REGEX,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis rule changed by timestamp."""
    service = get_service(environment=region, tenant_id=tenant)

    try:
        results = service.rules.query.changes_since(
            timestamp=timestamp,
            event_type=event_type,
            rule_type=rule_type,
        )
    except GraphQLNoRowsInResultSetError:  # pragma: no cover
        results = []

    normalized_results = TaegisRulesNormalizer(
        raw_results=results,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command("search")
@tracing
def rules_search(
    cell: Annotated[str, typer.Option()],
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis rule changed by timestamp."""
    service = get_service(environment=region, tenant_id=tenant)

    page = 1
    per_page = 500
    rules = []

    log.info(f"Getting page: {page}")
    try:
        with service(output=multiple_rules_output()):
            results = service.rules.query.search_rules(
                SearchRulesInput(query=cell),
                page=page,
                count=per_page,
            )
    except GraphQLNoRowsInResultSetError:  # pragma: no cover
        results = []
    log.debug(f"Results returned: {len(results)}")
    rules.extend(results)

    while len(results) == per_page:
        page += 1
        log.info(f"Getting page: {page}")
        try:
            with service(output=multiple_rules_output()):
                results = service.rules.query.search_rules(
                    SearchRulesInput(query=cell),
                    page=page,
                    count=per_page,
                )
        except GraphQLNoRowsInResultSetError:  # pragma: no cover
            results = []
        log.debug(f"Results returned: {len(results)}")
        rules.extend(results)

    normalized_results = TaegisRulesSearchNormalizer(
        raw_results=rules,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="enable")
@tracing
def rules_enable(
    rule_id: Annotated[str, typer.Option()],
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Enable a Taegis rule."""

    service = get_service(environment=region, tenant_id=tenant)

    results = service.rules.mutation.enable_rule(id_=rule_id)

    normalized_results = TaegisRuleNormalizer(
        raw_results=results,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command(name="disable")
@tracing
def rules_disable(
    rule_id: Annotated[str, typer.Option()],
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Disable a Taegis rule."""

    service = get_service(environment=region, tenant_id=tenant)

    results = service.rules.mutation.disable_rule(id_=rule_id)

    normalized_results = TaegisRuleNormalizer(
        raw_results=results,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@create_rule.command(name="custom")
@tracing
def rules_create_custom_rule(
    cell: Annotated[str, typer.Option()],
    name: Annotated[str, typer.Option()],
    description: Annotated[str, typer.Option()],
    severity: Annotated[RuleSeverity, typer.Option()],
    mitre_category: Annotated[Optional[List[str]], typer.Option()] = None,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis Create Custom QL Rule."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.rules.mutation.create_custom_ql_rule(
        input_=RuleInput(
            name=name,
            description=description,
            severity=RULE_SEVERTIY_MAP[severity.value],
            create_alert=True,
            attack_categories=mitre_category,
        ),
        ql_filter=RuleQLFilterInput(query=cell),
    )

    normalized_results = TaegisRuleNormalizer(
        raw_results=results,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@create_rule.command(name="suppression")
@tracing
def rules_create_suppression_rule(
    name: Annotated[str, typer.Option()],
    description: Annotated[str, typer.Option()],
    key: Annotated[List[RuleSuppressionKey], typer.Option()],
    pattern: Annotated[
        List[str],
        typer.Option(help="Regex pattern match, ensure you escape literals (ie '\\.')"),
    ],
    inverted: Annotated[
        Optional[List[str]],
        typer.Option(
            help="required for all if a single filter is inverted, set to True/true, all other values will be False"
        ),
    ] = None,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Taegis Create Suppression Rule."""
    service = get_service(environment=region, tenant_id=tenant)

    if len(key) != len(pattern):  # pragma: no cover
        raise BadOptionUsage(..., "key and pattern lengths must match")

    if inverted and len(key) != len(inverted):  # pragma: no cover
        raise BadOptionUsage(..., "key and inverted lengths must match")

    if inverted:
        inverted = [True if i.lower() == "true" else False for i in inverted]
    else:
        inverted = [False for _ in key]

    entity_prefixes = service.rules.query.entity_prefixes()
    filters = []

    for k, p, i in zip(key, pattern, inverted):
        if k == RuleSuppressionKey.entity:
            if ":" not in p:  # pragma: no cover
                raise BadOptionUsage(
                    ...,
                    f"'entity' patterns must be formatted 'prefix:pattern', prefix must be in value: {list(entity_prefixes.keys())}",
                )

            entity_prefix = p.split(":", maxsplit=1)[0]

            if entity_prefix not in entity_prefixes:  # pragma: no cover
                raise BadOptionUsage(
                    ...,
                    f"'entity' patterns must be formatted 'prefix:pattern', prefix must be in value: {list(entity_prefixes.keys())}",
                )

        filters.append(
            RuleFilterInput(
                key=RULE_SUPPRESSION_KEY_MAP[k.value], pattern=p, inverted=i
            )
        )

    results = service.rules.mutation.create_custom_suppression_rule(
        input_=RuleInput(
            name=name,
            description=description,
        ),
        filters=filters,
    )

    normalized_results = TaegisRuleNormalizer(
        raw_results=results,
        service="rules",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results
