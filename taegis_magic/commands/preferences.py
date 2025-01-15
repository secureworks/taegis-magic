"""Taegis Magic preferences commands."""

import logging
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional

import typer
from dataclasses_json import dataclass_json
from taegis_magic.core.log import tracing
from taegis_magic.core.normalizer import TaegisResultsNormalizer

from taegis_magic.core.service import get_service

log = logging.getLogger(__name__)

app = typer.Typer(help="Taegis Preferences Commands.")


@dataclass_json
@dataclass
class PreferenceResultsNormalizer(TaegisResultsNormalizer):
    @property
    def results(self) -> List[Dict[str, Any]]:
        if not self.raw_results:
            return []

        if not isinstance(self.raw_results, list):
            return [asdict(self.raw_results)]

        return [asdict(preference) for preference in self.raw_results]


@app.command()
@tracing
def parent(region: Optional[str] = None):
    """
    Parent Preferences.

    Parameters
    ----------
    region : Optional[str], optional
        Taegis region, by default None

    Returns
    -------
    PreferenceResultsNormalizer
        PreferenceResultsNormalizer
    """
    service = get_service(environment=region)

    preferences = service.preferences.query.partner_preferences()

    results = PreferenceResultsNormalizer(
        service="preferences",
        tenant_id=service.tenant_id,
        region=service.environment,
        raw_results=preferences,
        arguments={"region": service.environment},
    )

    return results


@app.command()
@tracing
def ticketing_settings(region: Optional[str] = None):
    """
    Ticketing Settings Preferences.

    Parameters
    ----------
    region : Optional[str], optional
        Taegis region, by default None

    Returns
    -------
    PreferenceResultsNormalizer
        PreferenceResultsNormalizer
    """
    service = get_service(environment=region)

    settings = service.preferences.query.ticketing_settings()

    results = PreferenceResultsNormalizer(
        service="preferences",
        tenant_id=service.tenant_id,
        region=service.environment,
        raw_results=settings,
        arguments={"region": service.environment},
    )

    return results


@app.command()
@tracing
def user_preference(key: str, region: Optional[str] = None):
    """
    User Preferences by Key.

    Parameters
    ----------
    key : str
        Preference key
    region : Optional[str], optional
        Taegis region, by default None

    Returns
    -------
    PreferenceResultsNormalizer
        PreferenceResultsNormalizer
    """
    service = get_service(environment=region)

    preferences = service.preferences.query.user_preference_by_key(key=key)

    results = PreferenceResultsNormalizer(
        service="preferences",
        tenant_id=service.tenant_id,
        region=service.environment,
        raw_results=preferences,
        arguments={"key": key, "region": service.environment},
    )

    return results
