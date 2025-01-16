"""Taegis Magic tenant-profiles commands."""

import inspect
import logging
import warnings
from dataclasses import asdict, field
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional

import pandas as pd
import numpy as np
import typer
from taegis_magic.core.callbacks import verify_file
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer
from taegis_magic.core.service import get_service
from taegis_sdk_python import GraphQLNoRowsInResultSetError
from taegis_sdk_python.services.tenant_profiles.types import (
    CriticalContactMtpInput,
    CustomerContactPreferenceMtp,
    MfaAccessCreateMtpInput,
    MfaAccessMtp,
    MfaAccessUpdateMtpInput,
    MfaServiceMtp,
    MtpNetworkType,
    NetworkRangeCreateMtpInput,
    NetworkRangeMtp,
    NetworkRangeUpdateMtpInput,
    SecurityControlCreateMtpInput,
    SecurityControlMtp,
    SecurityControlServiceMtp,
    SecurityControlSourceMtp,
    SecurityControlUpdateMtpInput,
)
from typing_extensions import Annotated

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Tenant Profile Commands.")
contacts = typer.Typer(help="Manage tenant profile contacts.")
network = typer.Typer(help="Manage tenant profile network ranges.")
network_template = typer.Typer(help="Manager tenant network range with template files.")
note = typer.Typer(help="Manage tenant profile notes.")
security_controls = typer.Typer(
    help="Manage tenant profile security control device information.",
)
mfa = typer.Typer(help="Manage tenant profile multifactor authentication information.")


network.add_typer(network_template, name="template")
app.add_typer(contacts, name="contacts")
app.add_typer(network, name="network")
app.add_typer(note, name="note")
app.add_typer(security_controls, name="security-controls")
app.add_typer(mfa, name="mfa")


def excel_normalize(value):
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    return value


class TaegisTenantProfileResultNormalizer(TaegisResultsNormalizer):
    """Tenant Profiles single response normalizer."""

    raw_results: Any = field(default=None)

    @property
    def results(self):
        return [asdict(self.raw_results)]


class TaegisTenantProfileResultsNormalizer(TaegisResultsNormalizer):
    """Tenant Profiles response list normalizer."""

    raw_results: List[Any] = field(default_factory=list)

    @property
    def results(self):
        return [asdict(r) for r in self.raw_results]


@app.command(name="list")
@tracing
def tenant_profile_list(
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """List a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.query.managed_tenant_profile()

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@contacts.command(name="list")
@tracing
def contacts_list(
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """List contacts in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.query.cse_contacts_mtp()

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@contacts.command(name="add")
@tracing
def contacts_add(
    user_id: Annotated[str, typer.Option()],
    preference: Annotated[CustomerContactPreferenceMtp, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Add a contact in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.create_critical_contact_mtp(
        CriticalContactMtpInput(
            tdr_user_id=user_id,
            preference=preference,
        )
    )

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@contacts.command(name="update")
@tracing
def contacts_update(
    id_: Annotated[str, typer.Option("--id")],
    user_id: Annotated[Optional[str], typer.Option()] = None,
    preference: Annotated[
        Optional[CustomerContactPreferenceMtp], typer.Option()
    ] = None,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Update a contact in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.update_critical_contact_mtp(
        id_=id_,
        input_=CriticalContactMtpInput(
            tdr_user_id=user_id,
            preference=preference,
        ),
    )

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@contacts.command(name="remove")
@tracing
def contacts_remove(
    id_: Annotated[Optional[str], typer.Option("--id")],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Remove a contact in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.delete_critical_contact_mtp(id_=id_)

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@network.command(name="list")
@tracing
def network_list(
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """List network ranges in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.query.network_ranges_mtp()

    normalized_results = TaegisTenantProfileResultsNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@network.command(name="add")
@tracing
def network_add(
    cidr: Annotated[str, typer.Option()],
    description: Annotated[str, typer.Option()],
    network_type: Annotated[MtpNetworkType, typer.Option()],
    is_critical: Annotated[bool, typer.Option()] = False,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Add a network range to tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.create_network_range_mtp(
        NetworkRangeCreateMtpInput(
            cidr=cidr,
            description=description,
            is_critical=is_critical,
            network_type=network_type,
        )
    )

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@network.command(name="update")
@tracing
def network_update(
    id_: Annotated[str, typer.Option("--id")],
    cidr: Annotated[Optional[str], typer.Option()] = None,
    description: Annotated[Optional[str], typer.Option()] = None,
    network_type: Annotated[Optional[MtpNetworkType], typer.Option()] = None,
    is_critical: Annotated[Optional[bool], typer.Option()] = None,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Update a network range in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.update_network_range_mtp(
        id_=id_,
        network=NetworkRangeUpdateMtpInput(
            cidr=cidr,
            description=description,
            is_critical=is_critical,
            network_type=network_type,
        ),
    )

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@network.command(name="remove")
@tracing
def network_remove(
    id_: Annotated[Optional[str], typer.Option("--id")],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Remove a network range in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    try:
        results = service.tenant_profiles.mutation.delete_network_range_mtp(id_=id_)
    except GraphQLNoRowsInResultSetError:
        results = NetworkRangeMtp()

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@note.command(name="list")
@tracing
def note_list(
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """List the note in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.query.note_mtp()

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@note.command(name="update")
@tracing
def note_update(
    contents: Annotated[str, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Update the note in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.update_note_mtp(contents=contents)

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@security_controls.command(name="list")
@tracing
def security_controls_list(
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """List security controls in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.query.security_controls_mtp()

    normalized_results = TaegisTenantProfileResultsNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@security_controls.command(name="add")
@tracing
def security_controls_add(
    ip: Annotated[str, typer.Option()],
    details: Annotated[str, typer.Option()],
    service_: Annotated[SecurityControlServiceMtp, typer.Option("--service")],
    source: Annotated[SecurityControlSourceMtp, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Add a security control in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.create_security_control_mtp(
        SecurityControlCreateMtpInput(
            ip=ip,
            details=details,
            service=service_,
            source=source,
        )
    )

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@security_controls.command(name="update")
@tracing
def security_controls_update(
    id_: Annotated[str, typer.Option("--id")],
    ip: Annotated[str, typer.Option()],
    details: Annotated[str, typer.Option()],
    service_: Annotated[SecurityControlServiceMtp, typer.Option("--service")],
    source: Annotated[SecurityControlSourceMtp, typer.Option()],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Update a security control in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.update_security_control_mtp(
        id_=id_,
        input_=SecurityControlUpdateMtpInput(
            ip=ip,
            details=details,
            service=service_,
            source=source,
        ),
    )

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@security_controls.command(name="remove")
@tracing
def security_controls_remove(
    id_: Annotated[str, typer.Option("--id")],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Remove a security control in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    try:
        results = service.tenant_profiles.mutation.delete_security_control_mtp(id_=id_)
    except GraphQLNoRowsInResultSetError:
        results = SecurityControlMtp()

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@mfa.command(name="list")
@tracing
def mfa_list(
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """List MFA access in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.query.mfa_accesses_mtp()

    normalized_results = TaegisTenantProfileResultsNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@mfa.command(name="add")
@tracing
def mfa_add(
    ip: Annotated[Optional[str], typer.Option()],
    exceptions: Annotated[Optional[str], typer.Option()],
    details: Annotated[Optional[str], typer.Option()],
    service_: Annotated[Optional[MfaServiceMtp], typer.Option("--service")],
    mfa_required: Annotated[Optional[bool], typer.Option()] = False,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Add a MFA access in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.create_mfa_access_mtp(
        MfaAccessCreateMtpInput(
            ip=ip,
            mfa_required=mfa_required,
            exceptions=exceptions,
            details=details,
            service=service_,
        )
    )

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@mfa.command(name="update")
@tracing
def mfa_update(
    id_: Annotated[Optional[str], typer.Option("--id")],
    ip: Annotated[Optional[str], typer.Option()],
    exceptions: Annotated[Optional[str], typer.Option()],
    details: Annotated[Optional[str], typer.Option()],
    service_: Annotated[Optional[MfaServiceMtp], typer.Option("--service")],
    mfa_required: Annotated[Optional[bool], typer.Option()] = False,
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Update a MFA access in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.mutation.update_mfa_access_mtp(
        id_=id_,
        input_=MfaAccessUpdateMtpInput(
            ip=ip,
            mfa_required=mfa_required,
            exceptions=exceptions,
            details=details,
            service=service_,
        ),
    )

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@mfa.command(name="remove")
@tracing
def mfa_remove(
    id_: Annotated[Optional[str], typer.Option("--id")],
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Remove a MFA access in a tenant profile."""
    service = get_service(environment=region, tenant_id=tenant)

    try:
        results = service.tenant_profiles.mutation.delete_mfa_access_mtp(
            id_=id_,
        )
    except GraphQLNoRowsInResultSetError:
        results = MfaAccessMtp()

    normalized_results = TaegisTenantProfileResultNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@network_template.command("generate")
@tracing
def network_template_generate(
    filename: Annotated[
        Path,
        typer.Option(
            exists=False,
            file_okay=True,
            dir_okay=False,
            readable=False,
            writable=True,
            resolve_path=True,
            callback=verify_file,
        ),
    ] = Path("taegis_network_range_template.xlsx"),
):
    """Generate a template file for tenant profiles network ranges."""

    input_ = NetworkRangeCreateMtpInput(
        cidr="192.0.2.0/24",
        description="RFC 5737 TEST-NET-1",
        network_type=MtpNetworkType.OTHER,
        is_critical=False,
    )
    df = pd.json_normalize([asdict(input_)], max_level=3)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = df.applymap(excel_normalize, na_action="ignore")

    df.to_excel(filename, index=False)

    normalized_results = TaegisResultsNormalizer(
        raw_results=[{"filename": str(filename.resolve())}],
        service="tenant_profiles",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@network_template.command("export")
@tracing
def network_template_export(
    filename: Annotated[
        Path,
        typer.Option(
            exists=False,
            file_okay=True,
            dir_okay=False,
            readable=False,
            writable=True,
            resolve_path=True,
            callback=verify_file,
        ),
    ] = Path("taegis_network_range_export.xlsx"),
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Export tenant profiles network ranges to a file."""
    service = get_service(environment=region, tenant_id=tenant)

    results = service.tenant_profiles.query.network_ranges_mtp()
    df = pd.json_normalize([asdict(result) for result in results], max_level=3)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = df.applymap(excel_normalize, na_action="ignore")

    df.to_excel(filename, index=False)

    normalized_results = TaegisResultsNormalizer(
        raw_results=[{"filename": str(filename.resolve())}],
        service="tenant_profiles",
        tenant_id="N/A",
        region="N/A",
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results


@network_template.command("upload")
@tracing
def network_template_upload(
    filename: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            writable=False,
            resolve_path=True,
        ),
    ] = Path("taegis_network_range_template.xlsx"),
    tenant: Annotated[Optional[str], typer.Option()] = None,
    region: Annotated[Optional[str], typer.Option()] = None,
):
    """Create/Update Tenant Profile Network Ranges."""
    service = get_service(environment=region, tenant_id=tenant)

    try:
        df = pd.read_excel(filename)
    except ValueError as exc:
        log.error(f"{filename.resolve()} is not an Excel file.")
        raise typer.Exit() from exc
    except FileNotFoundError as exc:
        log.error(f"{filename.resolve()} not found.")
        raise typer.Exit() from exc

    def excel_decode(row, networks):
        try:
            for network in networks:
                if network.cidr == row.cidr:
                    return NetworkRangeUpdateMtpInput(
                        cidr=row["cidr"],
                        description=row["description"],
                        is_critical=row["is_critical"],
                        network_type=MtpNetworkType(row["network_type"]),
                    )

            return NetworkRangeCreateMtpInput(
                cidr=row["cidr"],
                description=row["description"],
                is_critical=row["is_critical"],
                network_type=MtpNetworkType(row["network_type"]),
            )
        except ValueError as exc:
            if "MtpNetworkType" in exc.args[0]:
                log.error(
                    f"{exc}.  Use {[item.value for item in list(MtpNetworkType)]} instead."
                )
            else:
                log.error(exc)

            return np.nan
        except Exception as exc:
            log.error(exc)

            return np.nan

    networks = service.tenant_profiles.query.network_ranges_mtp()

    series = df.apply(excel_decode, axis=1, networks=networks).dropna()

    create_networks = [
        row for row in series if isinstance(row, NetworkRangeCreateMtpInput)
    ]
    update_networks = [
        row for row in series if isinstance(row, NetworkRangeUpdateMtpInput)
    ]

    for network in create_networks:
        try:
            results = service.tenant_profiles.mutation.create_network_range_mtp(network)
        except Exception as exc:
            log.error(exc)
            continue

    for network in update_networks:
        try:
            id_ = next(iter([n.id for n in networks if n.cidr == network.cidr]))
        except StopIteration:
            log.error(f"Could not find `id` for network: {network.cidr}")
            continue

        try:
            service.tenant_profiles.mutation.update_network_range_mtp(id_, network)
        except Exception as exc:
            log.error(exc)
            continue

    results = service.tenant_profiles.query.network_ranges_mtp()

    normalized_results = TaegisTenantProfileResultsNormalizer(
        raw_results=results,
        service="tenant_profiles",
        tenant_id=service.tenant_id,
        region=service.environment,
        arguments=inspect.currentframe().f_locals,
    )

    return normalized_results
