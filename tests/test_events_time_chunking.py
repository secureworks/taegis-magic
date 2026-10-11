"""Tests for taegis_magic.commands.events time chunked searches."""

from typing import Optional
from unittest.mock import MagicMock, patch

import pytest

from taegis_magic.commands.events import (
    ChunkedTaegisEventQueryNormalizer,
    TaegisEventQueryNormalizer,
    search,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def event_query_results(query_id: Optional[str], rows: int) -> MagicMock:
    """Build a minimal events query result page."""
    return MagicMock(
        query_id=query_id,
        next_=None,
        result=MagicMock(
            status="COMPLETE",
            rows=[{"id": f"{query_id}:{index}"} for index in range(rows)],
        ),
    )


@pytest.fixture
def chunk_results():
    """Return an events query result page set per time chunk.

    Each chunk returns two pages sharing the chunk's query identifier.
    """
    queries = []

    def _chunk_results(cell, region=None, tenant=None):
        queries.append(cell)
        query_id = f"query{len(queries)}"
        return [
            event_query_results(query_id, 6),
            event_query_results(query_id, 4),
        ]

    return _chunk_results


# ---------------------------------------------------------------------------
# ChunkedTaegisEventQueryNormalizer
# ---------------------------------------------------------------------------


def test_chunked_normalizer_counts_chunks_not_pages():
    normalizer = ChunkedTaegisEventQueryNormalizer(
        raw_results=[
            event_query_results("query1", 6),
            event_query_results("query1", 4),
            event_query_results(None, 2),
            event_query_results("query2", 5),
        ],
        service="events",
        tenant_id="tenant1",
        region="charlie",
    )

    assert normalizer.chunk_query_identifiers == ["query1", "query2"]
    assert normalizer.chunks == 2
    assert normalizer.results_returned == 17


def test_chunked_normalizer_tracks_raw_results_updates():
    normalizer = ChunkedTaegisEventQueryNormalizer(
        raw_results=[event_query_results("query1", 6)],
        service="events",
        tenant_id="tenant1",
        region="charlie",
    )

    assert normalizer.chunks == 1
    assert normalizer.results_returned == 6

    normalizer.raw_results.append(event_query_results("query2", 3))

    assert normalizer.chunks == 2
    assert normalizer.results_returned == 9


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


def test_search_time_chunked_combines_chunks(chunk_results):
    mock_share_link = MagicMock(id_="share-id")

    with (
        patch("taegis_magic.commands.events.get_service") as mock_get_service,
        patch(
            "taegis_magic.commands.events._event_query",
            side_effect=chunk_results,
        ) as mock_event_query,
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
            cell="FROM process WHERE commandline CONTAINS 'whoami'",
            time_window="21d",
            time_chunk="7d",
            track=False,
        )

        assert results.shareable_url == "\n".join(
            ["https://example.com/share/share-id"] * 3
        )

    assert isinstance(results, ChunkedTaegisEventQueryNormalizer)
    assert mock_event_query.call_count == 3
    assert results.chunks == 3
    assert results.results_returned == 30
    assert results.total_results == -1
    assert results.query.count("---") == 2
    assert results.arguments["time_window"] == "21d"
    assert results.arguments["time_chunk"] == "7d"


def test_search_time_chunked_tolerates_chunk_errors(chunk_results):
    def _side_effect(cell, region=None, tenant=None):
        if cell.startswith("FROM process"):
            raise ValueError("chunk failed")
        return chunk_results(cell, region=region, tenant=tenant)

    with (
        patch("taegis_magic.commands.events.get_service") as mock_get_service,
        patch(
            "taegis_magic.commands.events._event_query",
            side_effect=_side_effect,
        ),
    ):
        mock_get_service.return_value = MagicMock(
            tenant_id="tenant1", environment="charlie"
        )

        results = search(
            cell="FROM process WHERE commandline CONTAINS 'whoami'",
            time_window="2d",
            time_chunk="1d",
            track=False,
        )

    assert results.chunks == 0
    assert results.results_returned == 0
    assert results.shareable_url == "Not able to create shareable link"


def test_search_without_time_chunking_is_unchanged(chunk_results):
    with (
        patch("taegis_magic.commands.events.get_service") as mock_get_service,
        patch(
            "taegis_magic.commands.events._event_query",
            side_effect=chunk_results,
        ) as mock_event_query,
    ):
        mock_get_service.return_value = MagicMock(
            tenant_id="tenant1", environment="charlie"
        )

        results = search(
            cell="FROM process WHERE commandline CONTAINS 'whoami'",
            track=False,
        )

    assert mock_event_query.call_count == 1
    assert type(results) is TaegisEventQueryNormalizer
    assert results.query == "FROM process WHERE commandline CONTAINS 'whoami'"
    assert results.results_returned == 10
    assert "time_window" not in results.arguments


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
        search(cell="FROM process", track=False, **kwargs)
