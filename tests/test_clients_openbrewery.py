from unittest.mock import MagicMock, patch
from src.clients import openbrewery
import pytest
import json


def test_fetch_all_breweries_paginates_until_empty():
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"id": 1, "name": "Brewery"},
        {"id": 2, "name": "Brewery2"},
    ]
    mock_response.content = (
        b'[{"id": 1, "name": "Brewery"}, {"id": 2, "name": "Brewery2"}]'
    )
    mock_response.status_code = 200

    resp_page2 = MagicMock()
    resp_page2.content = b'[{"id": 3}]'
    resp_page2.status_code = 200

    resp_page3 = MagicMock()
    resp_page3.content = b"[]"
    resp_page3.status_code = 200

    session_mock = MagicMock()
    session_mock.get.side_effect = [mock_response, resp_page2, resp_page3]

    with patch.object(openbrewery, "_build_session", return_value=session_mock):
        batches = list(openbrewery.fetch_all_breweries(per_page=2, timeout_sec=5))

    assert len(batches) == 2
    assert sum(len(b) for b in batches) == 3


def test_fetch_all_breweries_handles_api_errors():
    session_mock = MagicMock()
    resp_ok = MagicMock()
    resp_ok.content = b'[{"id": 1}]'
    resp_ok.status_code = 200

    session_mock.get.side_effect = [resp_ok, Exception("API Error")]

    with patch.object(openbrewery, "_build_session", return_value=session_mock):
        with pytest.raises(Exception, match=".*API Error.*"):
            list(openbrewery.fetch_all_breweries(per_page=1, timeout_sec=5))


def test_fetch_all_breweries_empty_response():
    mock_response = MagicMock()
    mock_response.json.return_value = []
    mock_response.content = b"[]"
    mock_response.status_code = 200

    session_mock = MagicMock()
    session_mock.get.side_effect = [mock_response]

    with patch.object(openbrewery, "_build_session", return_value=session_mock):
        batches = list(openbrewery.fetch_all_breweries(per_page=2, timeout_sec=5))

    assert len(batches) == 0


def test_fetch_all_breweries_handles_timeout():
    session_mock = MagicMock()
    session_mock.get.side_effect = [
        MagicMock(content=b'[{"id": 1}]'),
        Exception("Timeout Error"),
    ]

    with patch.object(openbrewery, "_build_session", return_value=session_mock):
        with pytest.raises(Exception, match=".*Timeout Error.*"):
            list(openbrewery.fetch_all_breweries(per_page=1, timeout_sec=5))


def test_fetch_all_breweries_handles_unexpected_status_code():
    session_mock = MagicMock()
    session_mock.get.side_effect = [
        MagicMock(
            json=lambda: [{"id": 1}],
            raise_for_status=lambda: (_ for _ in ()).throw(
                Exception("Unexpected Status Code")
            ),
        )
    ]

    with patch.object(openbrewery, "_build_session", return_value=session_mock):
        with pytest.raises(Exception, match="Unexpected Status Code"):
            list(openbrewery.fetch_all_breweries(per_page=1, timeout_sec=5))


def test_fetch_breweries_metadata_parses_payload():
    session_mock = MagicMock()
    session_mock.get.return_value = MagicMock(
        json=lambda: {"total": 100, "page": 2, "per_page": 50},
        raise_for_status=lambda: None,
    )

    with patch.object(openbrewery, "_build_session", return_value=session_mock):
        meta = openbrewery.fetch_breweries_metadata(per_page=100, timeout_sec=5)

    assert meta == {"total": 100, "page": 2, "per_page": 50}


def test_fetch_breweries_metadata_handles_empty_payload():
    session_mock = MagicMock()
    session_mock.get.return_value = MagicMock(
        json=lambda: {}, raise_for_status=lambda: None
    )

    with patch.object(openbrewery, "_build_session", return_value=session_mock):
        meta = openbrewery.fetch_breweries_metadata(per_page=50, timeout_sec=5)
        assert meta == {"total": 0, "page": 1, "per_page": 50}


def test_fetch_breweries_metadata_handles_partial_payload():
    session_mock = MagicMock()
    session_mock.get.return_value = MagicMock(
        json=lambda: {"total": 100}, raise_for_status=lambda: None
    )

    with patch.object(openbrewery, "_build_session", return_value=session_mock):
        meta = openbrewery.fetch_breweries_metadata(per_page=50, timeout_sec=5)
        assert meta == {"total": 100, "page": 1, "per_page": 50}
