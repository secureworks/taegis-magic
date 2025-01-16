"""Taegis Magic users commands."""

import inspect
import logging
from dataclasses import asdict, field
from enum import Enum
from pprint import pprint
from typing import Any, List, Optional

import typer
from taegis_magic.commands.clients import ROLE_MAP, Roles
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer

from taegis_magic.core.service import get_service
from taegis_sdk_python.services.users.types import (
    InviteUsersResponse,
    SupportPinDetails,
    TDRUser,
    TDRUserInviteInput,
    TDRUsersSearchInput,
    TDRUsersSearchResults,
    TDRUserSupportPin,
    TDRUserTrialInviteInput,
    TDRUsersLanguage,
)
from typing_extensions import Annotated

app = typer.Typer(help="Taegis User Commands.")

log = logging.getLogger(__name__)


class TaegisUserResultsNormalizer(TaegisResultsNormalizer):
    raw_results: Any = field(default=None)

    @property
    def results(self):
        return [asdict(self.raw_results)]


class TaegisUsersResultsNormalizer(TaegisResultsNormalizer):
    raw_results: List[Any] = field(default_factory=list)

    @property
    def results(self):
        return [asdict(r) for r in self.raw_results]


class TaegisUserSearchResultsNormalizer(TaegisResultsNormalizer):
    raw_results: TDRUsersSearchResults = field(
        default_factory=lambda: TDRUsersSearchResults()
    )

    @property
    def results(self):
        return [asdict(user) for user in self.raw_results.results]


@app.command()
@tracing
def search(
    email: Annotated[Optional[List[str]], typer.Option()] = None,
    role: Annotated[Optional[List[Roles]], typer.Option()] = None,
    status: Optional[str] = None,
    tenant_status: Optional[str] = None,
    name: Optional[str] = None,
    tenant: Optional[str] = None,
    region: Optional[str] = None,
):
    """Search for users."""
    service = get_service(environment=region, tenant_id=tenant)

    if email:
        email_partials = [e for e in email if "%" in e]

        try:
            email_match = next(iter(email_partials))
        except StopIteration:
            email_match = None

        emails = [e for e in email if e not in email_partials]

        if email_match:
            if len(email_partials) > 1:
                log.warning(
                    "Only a single '%' email filter may be used at time.  Dropping remaining partials..."
                )

            if emails:
                log.warning(
                    "Cannot search for partial emails and full emails in the same search.  Dropping full emails..."
                )

            emails = None
    else:
        email_match = None
        emails = None

    if role:
        roles = [ROLE_MAP[r.value] for r in role]
    else:
        roles = None

    results = service.users.query.tdr_users_search(
        TDRUsersSearchInput(
            email=email_match,
            emails=emails,
            role_ids=roles,
            status=status,
            tenant_status=tenant_status,
            name=name,
        )
    )

    normalized_results = TaegisUserSearchResultsNormalizer(
        raw_results=results,
        service="users",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command()
@tracing
def support_pin(region: Optional[str] = None):
    """Get a PIN for Taegis support calls."""
    service = get_service(environment=region)

    results = service.users.query.get_support_pin()

    normalized_results = TaegisUserResultsNormalizer(
        raw_results=results,
        service="users",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command()
@tracing
def verify_support_pin(pin: str, region: Optional[str] = None):
    """Verify users associated with PIN for Taegis support calls."""
    service = get_service(environment=region)

    results = service.users.query.get_support_pin_verification(pin)

    normalized_results = TaegisUsersResultsNormalizer(
        raw_results=results,
        service="users",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command()
@tracing
def current_user(region: Optional[str] = None):
    """Get current user information."""
    service = get_service(environment=region)

    results = service.users.query.current_tdruser()

    normalized_results = TaegisUserResultsNormalizer(
        raw_results=results,
        service="users",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command()
@tracing
def invite_users(
    email: Annotated[List[str], typer.Option()],
    role: Annotated[Roles, typer.Option()] = Roles.analyst,
    language: Annotated[TDRUsersLanguage, typer.Option()] = TDRUsersLanguage.EN,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Invite users to Taegis."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.users.mutation.invite_tdr_users(
        invites=[
            TDRUserInviteInput(email=e, role_id=ROLE_MAP[role.value], language=language)
            for e in email
        ]
    )

    normalized_results = TaegisUsersResultsNormalizer(
        raw_results=results,
        service="users",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command()
@tracing
def invite_trial_user(
    email: Annotated[str, typer.Option()],
    trial_tenant: Annotated[str, typer.Option()],
    language: Annotated[TDRUsersLanguage, typer.Option()] = TDRUsersLanguage.EN,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Invite users to Taegis."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.users.mutation.invite_trial_tdr_user(
        invites=TDRUserTrialInviteInput(
            email=email,
            tenant=trial_tenant,
            language=language,
        )
    )

    normalized_results = TaegisUsersResultsNormalizer(
        raw_results=results,
        service="users",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@app.command()
@tracing
def validate_support_pin(email: str, pin: str, region: Optional[str] = None):
    """Validate a user's support pin."""
    service = get_service(environment=region)

    results = service.users.mutation.validate_support_pin(
        email=email,
        support_pin=pin,
    )

    normalized_results = TaegisUserResultsNormalizer(
        raw_results=results,
        service="users",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


if __name__ == "__main__":
    rv = app(standalone_mode=False)
    pprint(rv)
