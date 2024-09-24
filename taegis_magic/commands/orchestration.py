import inspect
import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import typer
from dataclasses_json import dataclass_json
from taegis_sdk_python.services.trigger_action.types import (
    ExecuteActionInput,
    PlaybookActions,
    PlaybookActionsV2Arguments,
    PlaybookExecution,
)
from typing_extensions import Annotated

from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResult, TaegisResultsNormalizer
from taegis_magic.core.service import get_service

log = logging.getLogger(__name__)

app = typer.Typer()
playbook_app = typer.Typer()
actions_app = typer.Typer()

app.add_typer(playbook_app, name="playbook")
playbook_app.add_typer(actions_app, name="actions")


@dataclass_json
@dataclass
class PlaybookActionsNormalizer(TaegisResultsNormalizer):
    """Playbook Actions Normalizer."""

    raw_results: List[PlaybookActions] = field(default_factory=list)

    @property
    def results(self) -> List[Dict[str, Any]]:
        """Results."""
        return [asdict(action) for action in self.raw_results]


@dataclass_json
@dataclass
class PlaybookExecutionWrapper:
    """Playbook Execution Wrapper."""

    id: str


@dataclass_json
@dataclass
class PlaybookExecutionNormalizer(TaegisResult):
    """Playbook Execution Normalizer."""

    raw_result: PlaybookExecutionWrapper = field(
        default_factory=lambda: PlaybookExecutionWrapper(id="")
    )

    @property
    def result(self) -> Dict[str, Any]:
        """Result."""
        return asdict(self.raw_result)

    @property
    def results(self) -> List[Dict[str, Any]]:
        """Results."""
        return [self.result]


@dataclass_json
@dataclass
class PlaybookExecutionLogsNormalizer(TaegisResultsNormalizer):
    """Playbook Execution Logs Normalizer."""

    raw_results: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def results(self) -> List[Dict[str, Any]]:
        """Results."""
        return self.raw_results


@actions_app.command("list")
@tracing
def list_playbook_actions(
    proactive_response_only: bool = True,
    page: Optional[int] = 1,
    per_page: Optional[int] = 50,
    region: Optional[str] = None,
    tenant_id: Optional[str] = None,
):
    """
    List Playbook Actions
    """
    service = get_service(environment=region, tenant_id=tenant_id)
    all_playbooks = []

    while True:
        playbooks = service.trigger_action.query.playbook_actions_v2(
            PlaybookActionsV2Arguments(
                proactive_response_only=proactive_response_only,
                page=page,
                per_page=per_page,
            )
        )

        if not playbooks.actions:
            break

        all_playbooks.extend(playbooks.actions)

        if len(playbooks.actions) < per_page:
            break

        page += 1

    results = PlaybookActionsNormalizer(
        service="trigger_action",
        tenant_id=service.tenant_id,
        region=service.environment,
        raw_results=all_playbooks,
        arguments=inspect.currentframe().f_locals,
    )

    return results


@actions_app.command("execute")
@tracing
def execute_playbook_action(
    playbook_action_id: str,
    target_resource_id: str,
    reason: Optional[str] = None,
    region: Optional[str] = None,
    tenant_id: Optional[str] = None,
):
    """
    Execute a Playbook Action
    """
    service = get_service(environment=region, tenant_id=tenant_id)

    input_data = ExecuteActionInput(
        playbook_action_id=playbook_action_id,
        target_resource_id=target_resource_id,
        reason=reason,
    )

    execution = service.trigger_action.mutation.execute_action(input_data)

    execution_wrapper = PlaybookExecutionWrapper(id=execution.id)

    result = PlaybookExecutionNormalizer(
        service="trigger_action",
        tenant_id=service.tenant_id,
        region=service.environment,
        raw_result=execution_wrapper,
        arguments=inspect.currentframe().f_locals,
    )

    return result


@playbook_app.command("logs")
@tracing
def get_playbook_execution_logs(
    playbook_execution_id: str,
    region: Optional[str] = None,
    tenant_id: Optional[str] = None,
):
    """
    Get Playbook Execution Logs.
    """
    service = get_service(environment=region, tenant_id=tenant_id)

    endpoint = "playbookExecutionLogs"
    variables = {"playbookExecutionId": playbook_execution_id}
    output = """
        id
        taskID
        parentID
        message
        children
        statusLogs
    """

    results = service.core.execute_query(
        endpoint=endpoint, variables=variables, output=output
    )
    results_json = results.get("playbookExecutionLogs", [])

    if not isinstance(results_json, list):
        raise TypeError("Results are not a list of dictionaries")

    normalized_results = PlaybookExecutionLogsNormalizer(
        service="playbooks",
        tenant_id=service.tenant_id,
        region=service.environment,
        raw_results=results_json,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


if __name__ == "__main__":
    app()
