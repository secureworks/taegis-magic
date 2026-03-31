"""Tests for taegis_magic.pandas.process_trees concurrent pipe operators."""

from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from taegis_magic.pandas.process_trees import lookup_children, lookup_lineage


@pytest.fixture
def sample_df():
    return pd.DataFrame(
        {
            "host_id": ["host-1", "host-2", "host-1"],
            "process_correlation_id": ["pcid-1", "pcid-2", "pcid-1"],
            "resource_id": ["rid-1", "rid-2", "rid-1"],
        }
    )


@pytest.fixture
def lineage_result():
    return SimpleNamespace(
        results=[
            {"lineage_index": 0, "process_name": "parent.exe"},
            {"lineage_index": 1, "process_name": "child.exe"},
        ]
    )


@pytest.fixture
def children_result():
    return SimpleNamespace(
        results=[
            {"process_name": "child-a.exe"},
            {"process_name": "child-b.exe"},
        ]
    )


class TestLookupLineage:
    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["host_id", "process_correlation_id", "resource_id"])
        result = lookup_lineage(df)
        assert result.empty

    def test_missing_columns_raises(self):
        df = pd.DataFrame({"host_id": ["h1"]})
        with pytest.raises(ValueError, match="must contain"):
            lookup_lineage(df)

    @patch("taegis_magic.pandas.process_trees.process_lineage")
    def test_concurrent_execution(self, mock_lineage, sample_df, lineage_result):
        mock_lineage.return_value = lineage_result

        result = lookup_lineage(sample_df, max_workers=2)

        assert "process_info.process_lineage" in result.columns
        assert "process_info.process_lineage.index" in result.columns
        assert not result.empty

    @patch("taegis_magic.pandas.process_trees.process_lineage")
    def test_deduplication(self, mock_lineage, sample_df, lineage_result):
        """Rows 0 and 2 share the same key tuple — API should be called only twice (2 unique keys)."""
        mock_lineage.return_value = lineage_result

        lookup_lineage(sample_df, max_workers=2)

        assert mock_lineage.call_count == 2

    @patch("taegis_magic.pandas.process_trees.process_lineage")
    def test_nan_keys_get_empty_results(self, mock_lineage, lineage_result):
        mock_lineage.return_value = lineage_result
        df = pd.DataFrame(
            {
                "host_id": ["host-1", None],
                "process_correlation_id": ["pcid-1", "pcid-2"],
                "resource_id": ["rid-1", "rid-2"],
            }
        )

        result = lookup_lineage(df, max_workers=2)

        # Only one unique non-null key
        assert mock_lineage.call_count == 1

    @patch("taegis_magic.pandas.process_trees.process_lineage")
    def test_api_failure_returns_empty(self, mock_lineage, sample_df):
        mock_lineage.side_effect = RuntimeError("API down")

        result = lookup_lineage(sample_df, max_workers=2)

        # Should not raise — failed rows get empty dicts
        assert not result.empty

    @patch("taegis_magic.pandas.process_trees.process_lineage")
    def test_explode_produces_correct_rows(self, mock_lineage, lineage_result):
        mock_lineage.return_value = lineage_result
        df = pd.DataFrame(
            {
                "host_id": ["host-1"],
                "process_correlation_id": ["pcid-1"],
                "resource_id": ["rid-1"],
            }
        )

        result = lookup_lineage(df, max_workers=1)

        # 1 row with 2 lineage results -> 2 rows after explode
        assert len(result) == 2
        assert result["process_info.process_lineage.index"].tolist() == [0, 1]


    @patch("taegis_magic.pandas.process_trees.process_lineage")
    def test_no_resource_id_column(self, mock_lineage, lineage_result):
        """resource_id is optional — lookup should work without it."""
        mock_lineage.return_value = lineage_result
        df = pd.DataFrame(
            {
                "host_id": ["host-1", "host-2"],
                "process_correlation_id": ["pcid-1", "pcid-2"],
            }
        )

        result = lookup_lineage(df, max_workers=2)

        assert "process_info.process_lineage" in result.columns
        assert mock_lineage.call_count == 2
        # resource_id should be passed as None to the API
        for call in mock_lineage.call_args_list:
            assert call.kwargs["resource_id"] is None

    @patch("taegis_magic.pandas.process_trees.process_lineage")
    def test_nan_resource_id_still_fetches(self, mock_lineage, lineage_result):
        """Rows with NaN resource_id should still be fetched (resource_id is optional)."""
        mock_lineage.return_value = lineage_result
        df = pd.DataFrame(
            {
                "host_id": ["host-1"],
                "process_correlation_id": ["pcid-1"],
                "resource_id": [None],
            }
        )

        result = lookup_lineage(df, max_workers=1)

        assert mock_lineage.call_count == 1
        assert mock_lineage.call_args.kwargs["resource_id"] is None
        assert not result.empty


class TestLookupChildren:
    def test_empty_dataframe(self):
        df = pd.DataFrame(columns=["host_id", "process_correlation_id", "resource_id"])
        result = lookup_children(df)
        assert result.empty

    def test_missing_columns_raises(self):
        df = pd.DataFrame({"host_id": ["h1"]})
        with pytest.raises(ValueError, match="must contain"):
            lookup_children(df)

    @patch("taegis_magic.pandas.process_trees.process_children")
    def test_concurrent_execution(self, mock_children, sample_df, children_result):
        mock_children.return_value = children_result

        result = lookup_children(sample_df, max_workers=2)

        assert "process_info.process_children" in result.columns
        assert not result.empty

    @patch("taegis_magic.pandas.process_trees.process_children")
    def test_deduplication(self, mock_children, sample_df, children_result):
        """Rows 0 and 2 share the same key tuple — API should be called only twice (2 unique keys)."""
        mock_children.return_value = children_result

        lookup_children(sample_df, max_workers=2)

        assert mock_children.call_count == 2

    @patch("taegis_magic.pandas.process_trees.process_children")
    def test_api_failure_returns_empty(self, mock_children, sample_df):
        mock_children.side_effect = RuntimeError("API down")

        result = lookup_children(sample_df, max_workers=2)

        assert not result.empty

    @patch("taegis_magic.pandas.process_trees.process_children")
    def test_explode_produces_correct_rows(self, mock_children, children_result):
        mock_children.return_value = children_result
        df = pd.DataFrame(
            {
                "host_id": ["host-1"],
                "process_correlation_id": ["pcid-1"],
                "resource_id": ["rid-1"],
            }
        )

        result = lookup_children(df, max_workers=1)

        # 1 row with 2 children -> 2 rows after explode
        assert len(result) == 2

    @patch("taegis_magic.pandas.process_trees.process_children")
    def test_no_resource_id_column(self, mock_children, children_result):
        """resource_id is optional — lookup should work without it."""
        mock_children.return_value = children_result
        df = pd.DataFrame(
            {
                "host_id": ["host-1"],
                "process_correlation_id": ["pcid-1"],
            }
        )

        result = lookup_children(df, max_workers=1)

        assert "process_info.process_children" in result.columns
        assert mock_children.call_args.kwargs["resource_id"] is None
        assert not result.empty
