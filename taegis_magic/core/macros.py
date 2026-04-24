"""Taegis Magic Tenant Macros.

Resolve @macro syntax in the --tenant flag to a list of tenant IDs
using the tenantsv4 API, with macro definitions stored in a YAML resource.
"""

import logging
from importlib.resources import files
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from taegis_magic.core.service import get_service
from taegis_sdk_python.config import get_config
from taegis_sdk_python.services.tenants4.types import (
    SubscriptionMatcher,
    TenantsQuery,
    TenantLabelMatcher,
)

log = logging.getLogger(__name__)

MACROS_SECTION = "magics.macros"
DEFAULT_MACROS_RESOURCE = files("taegis_magic.resources").joinpath("default_macros.yaml")


def _get_custom_macros_path() -> Optional[Path]:
    """Return a custom macros path from config, if set and valid."""
    config = get_config()

    if config.has_section(MACROS_SECTION) and config.has_option(
        MACROS_SECTION, "resource_path"
    ):
        custom_path = Path(config.get(MACROS_SECTION, "resource_path"))
        if custom_path.exists():
            return custom_path
        log.warning(
            "Custom macros path %s does not exist, falling back to default",
            custom_path,
        )

    return None


def load_macros(path: Optional[Path] = None) -> Dict[str, Any]:
    """Load macro definitions from a YAML file.

    Parameters
    ----------
    path
        Path to a custom YAML file.  When ``None``, checks the SDK
        config for a custom path, then falls back to the bundled
        default resource via ``importlib.resources``.

    Returns
    -------
    dict
        Mapping of macro name to its definition dict.
    """
    if path is not None:
        if not path.exists():
            log.warning("Macros file %s not found", path)
            return {}
        with open(path, "r") as fh:
            data = yaml.safe_load(fh) or {}
        return data.get("macros", {})

    custom_path = _get_custom_macros_path()
    if custom_path is not None:
        with open(custom_path, "r") as fh:
            data = yaml.safe_load(fh) or {}
        return data.get("macros", {})

    data = yaml.safe_load(DEFAULT_MACROS_RESOURCE.read_text()) or {}
    return data.get("macros", {})


def _build_tenants_queries(macro_def: Dict[str, Any]) -> List[TenantsQuery]:
    """Translate a user-friendly macro definition into TenantsQuery objects.

    The YAML format uses simple keys like ``services`` and ``labels``
    that are translated into the tenantsv4 ``TenantsQuery`` input type.

    Because tenantsv4 treats ``subscriptionsMatch`` as AND (all must
    match), multiple services are split into separate queries that are
    executed independently and their results unioned (OR semantics).
    """
    base_kwargs: Dict[str, Any] = {}


    services = macro_def.get("services")
    if not services:
        return [TenantsQuery(**base_kwargs)]

    # One query per service → union results (OR semantics)
    return [
        TenantsQuery(
            subscriptions_match=[SubscriptionMatcher(name=svc)],
            **base_kwargs,
        )
        for svc in services
    ]


def _fetch_tenant_ids_single(
    service,
    query: TenantsQuery,
) -> List[str]:
    """Execute a single tenantsv4 query with pagination.

    TenantsQuery is a frozen dataclass, so pagination creates
    a new query instance with the updated cursor on each page.
    """
    from dataclasses import replace

    tenant_ids: List[str] = []

    result = service.tenants4.query.tenants(tenants_query=query)
    tenant_ids.extend(t.id for t in (result.tenants or []))

    while result.has_more:
        query = replace(query, after_cursor=result.cursor_pos)
        result = service.tenants4.query.tenants(tenants_query=query)
        tenant_ids.extend(t.id for t in (result.tenants or []))

    return tenant_ids


def _fetch_tenant_ids(
    queries: List[TenantsQuery],
    region: Optional[str] = None,
) -> List[str]:
    """Execute tenantsv4 queries and return all matching tenant IDs.

    Multiple queries are executed independently and results are
    unioned (deduplicated) to achieve OR semantics across services.
    """
    service = get_service(environment=region)
    tenant_ids: set = set()

    for query in queries:
        tenant_ids.update(_fetch_tenant_ids_single(service, query))

    log.info("Macro resolved to %d unique tenant(s) from %d query(ies)",
             len(tenant_ids), len(queries))

    return list(tenant_ids)


def resolve_tenants(
    tenant: Optional[str],
    region: Optional[str] = None,
) -> List[Optional[str]]:
    """Resolve a tenant value, expanding macros if present.

    Parameters
    ----------
    tenant
        A tenant ID string, or a ``@macro`` reference.
        When ``None``, returns ``[None]`` (passthrough to default behaviour).
    region
        Taegis region for the tenantsv4 API call.

    Returns
    -------
    list
        A list of tenant ID strings.  For non-macro values this
        will be a single-element list.

    Raises
    ------
    ValueError
        If the macro name is not found in the YAML resource.
    """
    if tenant is None or not tenant.startswith("@"):
        return [tenant]

    macro_name = tenant[1:]
    log.info("Resolving tenant macro: @%s", macro_name)

    macros = load_macros()

    if macro_name not in macros:
        available = ", ".join(f"@{m}" for m in macros) or "(none defined)"
        raise ValueError(
            f"Unknown tenant macro '@{macro_name}'. "
            f"Available macros: {available}"
        )

    macro_def = macros[macro_name]
    queries = _build_tenants_queries(macro_def)
    tenant_ids = _fetch_tenant_ids(queries, region=region)

    if not tenant_ids:
        log.warning("Macro @%s resolved to zero tenants", macro_name)

    return tenant_ids
