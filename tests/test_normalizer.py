"""Tests for merge_normalizer_results in taegis_magic.core.normalizer."""

from taegis_magic.core.normalizer import (
    TaegisResultsNormalizer,
    merge_normalizer_results,
)


class TestMergeNormalizerResults:
    def test_empty_list_returns_empty_normalizer(self):
        result = merge_normalizer_results([])
        assert result.tenant_id == "None"
        assert result.results == []

    def test_single_result_returned_as_is(self):
        normalizer = TaegisResultsNormalizer(
            service="alerts",
            tenant_id="111",
            region="charlie",
            raw_results=[{"id": "a1"}],
        )
        result = merge_normalizer_results([normalizer])

        assert result is normalizer
        assert result.tenant_id == "111"

    def test_merges_multiple_results(self):
        n1 = TaegisResultsNormalizer(
            service="alerts",
            tenant_id="111",
            region="charlie",
            raw_results=[{"id": "a1"}, {"id": "a2"}],
        )
        n2 = TaegisResultsNormalizer(
            service="alerts",
            tenant_id="222",
            region="charlie",
            raw_results=[{"id": "b1"}],
        )

        result = merge_normalizer_results([n1, n2])

        assert result.tenant_id == "111, 222"
        assert result.service == "alerts"
        assert result.region == "charlie"
        assert len(result.results) == 3

    def test_tags_records_with_macro_tenant_id(self):
        n1 = TaegisResultsNormalizer(
            service="alerts",
            tenant_id="111",
            region="charlie",
            raw_results=[{"id": "a1"}],
        )
        n2 = TaegisResultsNormalizer(
            service="alerts",
            tenant_id="222",
            region="charlie",
            raw_results=[{"id": "b1"}],
        )

        result = merge_normalizer_results([n1, n2])

        assert result.results[0]["_macro_tenant_id"] == "111"
        assert result.results[1]["_macro_tenant_id"] == "222"

    def test_preserves_original_record_fields(self):
        n1 = TaegisResultsNormalizer(
            service="events",
            tenant_id="111",
            region="delta",
            raw_results=[{"id": "e1", "severity": "high"}],
        )

        result = merge_normalizer_results([n1, n1])

        for record in result.results:
            assert "id" in record
            assert "severity" in record
            assert record["severity"] == "high"
