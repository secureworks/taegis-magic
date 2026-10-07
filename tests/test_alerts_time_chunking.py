"""Tests for taegis_magic.commands.alerts time chunked searches."""

from unittest.mock import MagicMock, patch

import pytest

from taegis_magic.commands.alerts import (
    AlertsResultsNormalizer,
    ChunkedAlertsResultsNormalizer,
    search,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def alerts_response(
    total_results: int, returned: int, part: int = 1, query_id: str = None
) -> MagicMock:
    """Build a minimal alerts response."""
    return MagicMock(
        alerts=MagicMock(
            group_by=None,
            total_results=total_results,
            part=part,
            list_=[MagicMock() for _ in range(returned)],
        ),
        query_id=query_id,
    )


@pytest.fixture
def chunk_normalizer():
    """Return a single tenant search result per time chunk.

    Each chunk returns the search response and one polled part.
    """

    def _chunk_normalizer(cell, **kwargs):
        return AlertsResultsNormalizer(
            raw_results=[
                alerts_response(10, 6, part=1, query_id="query-id"),
                alerts_response(10, 4, part=2),
            ],
            service="alerts",
            tenant_id=kwargs.get("tenant_id") or "tenant1",
            region="charlie",
            query=cell,
        )

    return _chunk_normalizer


# ---------------------------------------------------------------------------
# ChunkedAlertsResultsNormalizer
# ---------------------------------------------------------------------------


def test_chunked_normalizer_counts_chunks_not_parts():
    normalizer = ChunkedAlertsResultsNormalizer(
        raw_results=[
            alerts_response(10, 6, part=1),
            alerts_response(10, 4, part=2),
            alerts_response(5, 5, part=1),
        ],
        service="alerts",
        tenant_id="tenant1",
        region="charlie",
    )

    assert normalizer.chunks == 2
    assert normalizer.chunk_total_results == 15
    assert normalizer.total_results == 15
    assert normalizer.results_returned == 15


def test_chunked_normalizer_tracks_raw_results_updates():
    normalizer = ChunkedAlertsResultsNormalizer(
        raw_results=[alerts_response(10, 10, part=1)],
        service="alerts",
        tenant_id="tenant1",
        region="charlie",
    )

    assert normalizer.chunks == 1
    assert normalizer.total_results == 10

    normalizer.raw_results.append(alerts_response(7, 7, part=1))

    assert normalizer.chunks == 2
    assert normalizer.total_results == 17


def test_chunked_normalizer_counts_empty_chunk():
    normalizer = ChunkedAlertsResultsNormalizer(
        raw_results=[alerts_response(0, 0, part=0)],
        service="alerts",
        tenant_id="tenant1",
        region="charlie",
    )

    assert normalizer.chunks == 1
    assert normalizer.total_results == 0


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


def test_search_time_chunked_combines_chunks(chunk_normalizer):
    mock_share_link = MagicMock(id_="share-id")

    with (
        patch("taegis_magic.commands.alerts.get_service") as mock_get_service,
        patch("taegis_magic.commands.alerts.resolve_tenants", return_value=["tenant1"]),
        patch(
            "taegis_magic.commands.alerts._search_single_tenant",
            side_effect=chunk_normalizer,
        ) as mock_search,
    ):
        mock_get_service.return_value = MagicMock(
            tenant_id="tenant1",
            environment="charlie",
            core=MagicMock(sync_url="https://api.example.com"),
        )
        mock_get_service.return_value.sharelinks.mutation.create_share_link.return_value = (
            mock_share_link
        )

        results = search(
            cell="FROM alert WHERE severity >= 0.6",
            time_window="21d",
            time_chunk="7d",
        )

        assert results.shareable_url == "\n".join(
            ["https://example.com/share/share-id"] * 3
        )

    assert isinstance(results, ChunkedAlertsResultsNormalizer)
    assert mock_search.call_count == 3
    assert results.chunks == 3
    assert results.total_results == 30
    assert results.results_returned == 30


def test_search_time_chunked_tolerates_chunk_errors(chunk_normalizer):
    def _side_effect(cell, **kwargs):
        if cell.endswith("| head 5"):
            raise ValueError("chunk failed")
        return chunk_normalizer(cell, **kwargs)

    with (
        patch("taegis_magic.commands.alerts.get_service") as mock_get_service,
        patch("taegis_magic.commands.alerts.resolve_tenants", return_value=["tenant1"]),
        patch(
            "taegis_magic.commands.alerts._search_single_tenant",
            side_effect=_side_effect,
        ),
    ):
        mock_get_service.return_value = MagicMock(
            tenant_id="tenant1", environment="charlie"
        )

        results = search(
            cell="FROM alert WHERE severity >= 0.6 | head 5",
            time_window="2d",
            time_chunk="1d",
        )

    assert results.chunks == 0
    assert results.total_results == -1
    assert results.results_returned == -1


def test_search_without_time_chunking_is_unchanged(chunk_normalizer):
    with (
        patch("taegis_magic.commands.alerts.resolve_tenants", return_value=["tenant1"]),
        patch(
            "taegis_magic.commands.alerts._search_single_tenant",
            side_effect=lambda *args, **kwargs: chunk_normalizer(args[0]),
        ) as mock_search,
    ):
        results = search(cell="FROM alert WHERE severity >= 0.6")

    assert mock_search.call_count == 1
    assert type(results) is AlertsResultsNormalizer
    assert results.total_results == 10


@pytest.mark.parametrize(
    "kwargs",
    [
        {"time_window": "30d"},
        {"time_chunk": "7d"},
        {"time_window": "30d", "time_chunk": "7d", "ai": True},
    ],
)
def test_search_rejects_invalid_time_chunk_options(kwargs):
    with pytest.raises(ValueError):
        search(cell="FROM alert", **kwargs)
