"""Taegis Magic clients commands."""

import logging
from dataclasses import field, asdict
from enum import Enum
from pprint import pprint
from typing import List, Optional, Union
import inspect
from typing_extensions import Annotated

import typer
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer

from taegis_magic.core.service import get_service
from taegis_sdk_python.services.clients.types import (
    Client,
    NewClient,
    ClientRoleAssignmentInput,
)

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Clients Commands.")
rotate_app = typer.Typer(help="Taegis Clients Rotate Commands.")
role_app = typer.Typer(help="Taegis Clients Role Commands.")
app.add_typer(rotate_app, name="rotate")
app.add_typer(role_app, name="role")


class Roles(str, Enum):
    """Role choices for client commands."""

    administrator = "administrator"
    analyst = "analyst"
    responder = "responder"
    auditor = "auditor"


ROLE_MAP = {
    "administrator": "ba0fdcbd-e87d-4bdd-ae7d-ca6118b25068",
    "analyst": "a4903f9f-465b-478f-a24e-82fa2e129d2e",
    "responder": "a72dace7-4536-4dbc-947d-015a8eb65f4d",
    "auditor": "ace1cae4-59fd-4fd1-9500-40077dc529a7",
}


class TaegisClientResultsNormalizer(TaegisResultsNormalizer):
    """Taegis Client Results normalizer."""

    raw_results: Union[Client, NewClient] = field(default_factory=lambda: Client())

    @property
    def results(self):
        return [asdict(self.raw_results)]


class TaegisClientsResultsNormalizer(TaegisResultsNormalizer):
    """Taegis Clients Results normalizer."""

    raw_results: List[Client] = field(default_factory=list)

    @property
    def results(self):
        return [asdict(result) for result in self.raw_results]


@app.command()
@tracing
def create(
    app_name: str,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
    role: Optional[List[Roles]] = None,
):
    """Create an OAuth2 client_id and client_secret for non-interactive login."""
    service = get_service(environment=region, tenant_id=tenant)
    roles = [ROLE_MAP[r.value] for r in role or [Roles.analyst]]

    results = service.clients.mutation.create_client(name=app_name, roles=roles)

    normalized_results = TaegisClientResultsNormalizer(
        raw_results=results,
        tenant_id=service.tenant_id,
        region=service.environment,
        service="clients",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command()
@tracing
def remove(
    client_id: Annotated[str, typer.Argument(help="client UUID or client_id")],
    region: Optional[str] = None,
    tenant: Optional[str] = None,
):
    """Delete an OAuth2 client_id."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.clients.mutation.delete_client(client_id)

    normalized_results = TaegisClientResultsNormalizer(
        raw_results=results,
        tenant_id=service.tenant_id,
        region=service.environment,
        service="clients",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command()
@tracing
def search(
    name: Optional[str] = None,
    client_id: Optional[List[str]] = None,
    role: Optional[List[Roles]] = None,
    region: Optional[str] = None,
    tenant: Optional[str] = None,
):
    """Search OAuth2 clients."""
    service = get_service(environment=region, tenant_id=tenant)
    if role:
        roles = [ROLE_MAP[r.value] for r in role]
    else:
        roles = None

    results = service.clients.query.clients(
        name=name, client_ids=client_id, role_ids=roles
    )

    normalized_results = TaegisClientsResultsNormalizer(
        raw_results=results,
        tenant_id=service.tenant_id,
        region=service.environment,
        service="clients",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@rotate_app.command(name="secret")
@tracing
def rotate_secret(
    client_id: str,
    region: Optional[str] = None,
    tenant: Optional[str] = None,
):
    """Rotate secret value for an OAuth2 client."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.clients.mutation.rotate_client_secret(client_id)

    normalized_results = TaegisClientResultsNormalizer(
        raw_results=results,
        tenant_id=service.tenant_id,
        region=service.environment,
        service="clients",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@role_app.command(name="append")
@tracing
def role_append(
    client_id: str,
    role: Annotated[List[Roles], typer.Option()],
    tenant: Annotated[str, typer.Option()],
    region: Optional[str] = None,
):
    """Add/Append a role to an OAuth2 client."""
    service = get_service(environment=region, tenant_id=tenant)
    roles = [ROLE_MAP[r.value] for r in role]

    results = service.clients.mutation.append_client_roles(
        client_id,
        roles=[
            ClientRoleAssignmentInput(tenant_id=tenant, role_id=role) for role in roles
        ],
    )

    normalized_results = TaegisClientResultsNormalizer(
        raw_results=results,
        tenant_id=service.tenant_id,
        region=service.environment,
        service="clients",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@role_app.command(name="remove")
@tracing
def role_remove(
    client_id: str,
    role: Annotated[List[Roles], typer.Option()],
    tenant: Annotated[str, typer.Option()],
    region: Optional[str] = None,
):
    """Remove a role for an OAuth2 client."""
    service = get_service(environment=region, tenant_id=tenant)
    roles = [ROLE_MAP[r.value] for r in role]

    results = service.clients.mutation.remove_client_roles(
        client_id,
        roles=roles,
    )

    normalized_results = TaegisClientResultsNormalizer(
        raw_results=results,
        tenant_id=service.tenant_id,
        region=service.environment,
        service="clients",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


if __name__ == "__main__":
    rv = app(standalone_mode=False)
    pprint(rv)
