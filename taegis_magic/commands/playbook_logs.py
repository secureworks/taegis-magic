import inspect
import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import typer
from dataclasses_json import dataclass_json
from taegis_sdk_python.services.trigger_action.types import PlaybookExecution
from typing_extensions import Annotated

from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.service import get_service

log = logging.getLogger(__name__)

app = typer.Typer()


@dataclass_json
@dataclass
class PlaybookExecutionLogsNormalizer(TaegisResultsNormalizer):
    """Playbook Execution Logs Normalizer."""

    raw_results: List[Dict[str, Any]] = field(default_factory=list)

    @property
    def results(self) -> List[Dict[str, Any]]:
        """Results."""
        return self.raw_results


@app.command()
@tracing
def get_playbook_execution_logs(
    playbook_execution_id: str,
    region: Optional[str] = None,
    tenant_id: Optional[str] = None,
):
    """
    Get Playbook Execution Logs.

    Parameters

    playbook_execution_id : str
        ID of the playbook execution
    region : Optional[str], optional
        Taegis region, by default None
    tenant_id : Optional[str], optional
        Tenant ID, by default None

    Returns
    PlaybookExecutionLogsNormalizer
        PlaybookExecutionLogsNormalizer
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
