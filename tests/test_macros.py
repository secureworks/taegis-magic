"""Tests for taegis_magic.core.macros — Tenant Macro resolution engine."""

from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from taegis_magic.core.macros import (
    DEFAULT_MACROS_RESOURCE,
    MACROS_SECTION,
    _build_tenants_queries,
    _fetch_tenant_ids,
    _get_custom_macros_path,
    load_macros,
    resolve_tenants,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_MACROS_YAML = {
    "macros": {
        "mdr": {
            "services": [
                "ManagedXDR Essentials",
                "MDR",
                "Dell SafeGuard MDR",
            ],
        }
    }
}


@pytest.fixture
def macros_yaml_file(tmp_path):
    """Write sample macros YAML to a temp file."""
    path = tmp_path / "macros.yaml"
    path.write_text(yaml.dump(SAMPLE_MACROS_YAML))
    return path


@pytest.fixture
def empty_yaml_file(tmp_path):
    path = tmp_path / "empty.yaml"
    path.write_text("")
    return path


# ---------------------------------------------------------------------------
# get_macros_config_path
# ---------------------------------------------------------------------------


class TestGetCustomMacrosPath:
    def test_returns_none_when_no_config(self):
        mock_config = MagicMock()
        mock_config.has_section.return_value = False

        with patch("taegis_magic.core.macros.get_config", return_value=mock_config):
            result = _get_custom_macros_path()

        assert result is None

    def test_returns_custom_path_when_configured(self, tmp_path):
        custom_file = tmp_path / "custom_macros.yaml"
        custom_file.write_text("macros: {}")

        mock_config = MagicMock()
        mock_config.has_section.return_value = True
        mock_config.has_option.return_value = True
        mock_config.get.return_value = str(custom_file)

        with patch("taegis_magic.core.macros.get_config", return_value=mock_config):
            result = _get_custom_macros_path()

        assert result == custom_file

    def test_returns_none_when_custom_path_missing(self):
        mock_config = MagicMock()
        mock_config.has_section.return_value = True
        mock_config.has_option.return_value = True
        mock_config.get.return_value = "/nonexistent/macros.yaml"

        with patch("taegis_magic.core.macros.get_config", return_value=mock_config):
            result = _get_custom_macros_path()

        assert result is None


# ---------------------------------------------------------------------------
# load_macros
# ---------------------------------------------------------------------------


class TestLoadMacros:
    def test_loads_macros_from_file(self, macros_yaml_file):
        macros = load_macros(macros_yaml_file)
        assert "mdr" in macros
        assert macros["mdr"]["services"] == [
            "ManagedXDR Essentials",
            "MDR",
            "Dell SafeGuard MDR",
        ]

    def test_returns_empty_for_missing_file(self, tmp_path):
        macros = load_macros(tmp_path / "does_not_exist.yaml")
        assert macros == {}

    def test_returns_empty_for_empty_file(self, empty_yaml_file):
        macros = load_macros(empty_yaml_file)
        assert macros == {}

    def test_loads_default_macros_resource(self):
        """Loading with no args and no custom config uses the bundled resource."""
        mock_config = MagicMock()
        mock_config.has_section.return_value = False

        with patch("taegis_magic.core.macros.get_config", return_value=mock_config):
            macros = load_macros()

        assert "mdr" in macros
        assert "services" in macros["mdr"]


# ---------------------------------------------------------------------------
# _build_tenants_query
# ---------------------------------------------------------------------------


class TestBuildTenantsQueries:
    def test_services_produce_one_query_per_service(self):
        """Multiple services → one query each (OR semantics)."""
        macro_def = {"services": ["MDR", "MXDR POC"]}
        queries = _build_tenants_queries(macro_def)

        assert len(queries) == 2
        assert queries[0].subscriptions_match[0].name == "MDR"
        assert queries[1].subscriptions_match[0].name == "MXDR POC"

    def test_single_service_produces_single_query(self):
        macro_def = {"services": ["MDR"]}
        queries = _build_tenants_queries(macro_def)

        assert len(queries) == 1
        assert len(queries[0].subscriptions_match) == 1
        assert queries[0].subscriptions_match[0].name == "MDR"

    def test_empty_macro_returns_single_empty_query(self):
        queries = _build_tenants_queries({})
        assert len(queries) == 1
        assert queries[0].subscriptions_match is None
        assert queries[0].names is None

    def test_services_with_base_filters_propagated(self):
        """Base filters like enabled should appear on every per-service query."""
        macro_def = {
            "services": ["MDR", "MXDR POC"]
        }
        queries = _build_tenants_queries(macro_def)
        assert len(queries) == 2


# ---------------------------------------------------------------------------
# resolve_tenants
# ---------------------------------------------------------------------------


@dataclass
class FakeTenantV4:
    id: str
    name: str = ""


@dataclass
class FakeTenantResults:
    tenants: list
    has_more: bool = False
    cursor_pos: str = None
    count: int = 0


class TestResolveTenants:
    def test_passthrough_for_none(self):
        result = resolve_tenants(None)
        assert result == [None]

    def test_passthrough_for_plain_id(self):
        result = resolve_tenants("12345")
        assert result == ["12345"]

    def test_passthrough_for_non_macro_string(self):
        result = resolve_tenants("my-tenant")
        assert result == ["my-tenant"]

    @patch("taegis_magic.core.macros.load_macros")
    @patch("taegis_magic.core.macros._fetch_tenant_ids")
    def test_resolves_macro(self, mock_fetch, mock_load):
        mock_load.return_value = {
            "mdr": {"services": ["MDR"]},
        }
        mock_fetch.return_value = ["111", "222", "333"]

        result = resolve_tenants("@mdr", region="charlie")

        assert result == ["111", "222", "333"]
        mock_fetch.assert_called_once()

    @patch("taegis_magic.core.macros.load_macros")
    def test_raises_for_unknown_macro(self, mock_load):
        mock_load.return_value = {"mdr": {"services": ["MDR"]}}

        with pytest.raises(ValueError, match="Unknown tenant macro '@nope'"):
            resolve_tenants("@nope")

    @patch("taegis_magic.core.macros.load_macros")
    @patch("taegis_magic.core.macros._fetch_tenant_ids")
    def test_returns_empty_list_for_no_matches(self, mock_fetch, mock_load):
        mock_load.return_value = {"empty": {"names": ["DoesNotExist*"]}}
        mock_fetch.return_value = []

        result = resolve_tenants("@empty")

        assert result == []


# ---------------------------------------------------------------------------
# _fetch_tenant_ids (with pagination)
# ---------------------------------------------------------------------------


class TestFetchTenantIds:
    @patch("taegis_magic.core.macros.get_service")
    def test_single_query_single_page(self, mock_get_service):
        mock_service = MagicMock()
        mock_get_service.return_value = mock_service

        mock_service.tenants4.query.tenants.return_value = FakeTenantResults(
            tenants=[FakeTenantV4(id="100"), FakeTenantV4(id="200")],
            has_more=False,
        )

        queries = _build_tenants_queries({"services": ["MDR"]})
        result = _fetch_tenant_ids(queries, region="charlie")

        assert set(result) == {"100", "200"}
        assert len(result) == 2
        mock_service.tenants4.query.tenants.assert_called_once()

    @patch("taegis_magic.core.macros.get_service")
    def test_pagination(self, mock_get_service):
        mock_service = MagicMock()
        mock_get_service.return_value = mock_service

        page1 = FakeTenantResults(
            tenants=[FakeTenantV4(id="100")],
            has_more=True,
            cursor_pos="cursor-1",
        )
        page2 = FakeTenantResults(
            tenants=[FakeTenantV4(id="200")],
            has_more=False,
        )
        mock_service.tenants4.query.tenants.side_effect = [page1, page2]

        queries = _build_tenants_queries({"services": ["MDR"]})
        result = _fetch_tenant_ids(queries, region="charlie")

        assert set(result) == {"100", "200"}
        assert len(result) == 2
        assert mock_service.tenants4.query.tenants.call_count == 2

    @patch("taegis_magic.core.macros.get_service")
    def test_multiple_services_unioned_and_deduplicated(self, mock_get_service):
        """Two services that share a tenant → deduplicated in output."""
        mock_service = MagicMock()
        mock_get_service.return_value = mock_service

        result_mdr = FakeTenantResults(
            tenants=[FakeTenantV4(id="100"), FakeTenantV4(id="200")],
            has_more=False,
        )
        result_mxdr = FakeTenantResults(
            tenants=[FakeTenantV4(id="200"), FakeTenantV4(id="300")],
            has_more=False,
        )
        mock_service.tenants4.query.tenants.side_effect = [result_mdr, result_mxdr]

        queries = _build_tenants_queries({"services": ["MDR", "MXDR POC"]})
        result = _fetch_tenant_ids(queries, region="charlie")

        assert set(result) == {"100", "200", "300"}
        assert len(result) == 3
        assert mock_service.tenants4.query.tenants.call_count == 2
