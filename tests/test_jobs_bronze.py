import json
from unittest.mock import patch

import pytest

from src.jobs.bronze import _create_directories, extract_to_bronze


@pytest.mark.parametrize(
    "run_id,expected_folder",
    [
        (
            "manual__2026-03-08T22:53:53+00:00",
            "run_id=manual__2026-03-08T22_53_53_00_00",
        ),
        ("simple_run", "run_id=simple_run"),
    ],
)
def test_extract_to_bronze_writes_jsonl_and_manifest_with_metadata(
    tmp_path, run_id, expected_folder
):
    fake_batches = [
        [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}],
        [{"id": 3, "name": "C"}],
    ]

    with patch("src.jobs.bronze.fetch_all_breweries", return_value=fake_batches), patch(
        "src.jobs.bronze.fetch_breweries_metadata", return_value={"total": 3}
    ):
        extract_to_bronze(tmp_path, ds="2026-03-01", run_id=run_id)

    out_dir = (
        tmp_path
        / "bronze"
        / "breweries"
        / "ingestion_date=2026-03-01"
        / expected_folder
    )

    data_path = out_dir / "breweries.jsonl"
    manifest_path = out_dir / "manifest.json"

    assert data_path.exists()
    assert manifest_path.exists()

    lines = data_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert [json.loads(line)["id"] for line in lines] == [1, 2, 3]

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest == {
        "ingestion_date": "2026-03-01",
        "run_id": run_id,
        "records": 3,
        "source_expected_total": 3,
    }


def test_extract_to_bronze_preserves_utf8_characters(tmp_path):
    fake_batches = [
        [{"id": 1, "name": "Cervejaria São José", "state_province": "Kärnten"}]
    ]

    with patch("src.jobs.bronze.fetch_all_breweries", return_value=fake_batches), patch(
        "src.jobs.bronze.fetch_breweries_metadata", return_value={"total": 1}
    ):
        extract_to_bronze(tmp_path, ds="2026-03-01", run_id="utf8_run")

    data_path = (
        tmp_path
        / "bronze"
        / "breweries"
        / "ingestion_date=2026-03-01"
        / "run_id=utf8_run"
        / "breweries.jsonl"
    )
    content = data_path.read_text(encoding="utf-8")

    assert "São José" in content
    assert "Kärnten" in content


def test_create_directories_creates_missing_base_path(tmp_path):
    base_path = tmp_path / "missing_data_root"

    out_dir = _create_directories(base_path, ds="2026-03-01", run_id="run:1+test")

    assert base_path.exists()
    assert out_dir.exists()
    assert out_dir.name == "run_id=run_1_test"


@patch("src.jobs.bronze.os.access", return_value=False)
def test_create_directories_raises_when_base_path_is_not_writable(
    _mock_access, tmp_path
):
    base_path = tmp_path / "data"
    base_path.mkdir(parents=True, exist_ok=True)

    with pytest.raises(PermissionError, match="Base path is not writable"):
        _create_directories(base_path, ds="2026-03-01", run_id="run_1")


def test_extract_to_bronze_calls_metadata_once_after_rows_are_written(tmp_path):
    fake_batches = [[{"id": 1}], [{"id": 2}]]

    with patch(
        "src.jobs.bronze.fetch_all_breweries", return_value=fake_batches
    ) as mock_fetch_rows, patch(
        "src.jobs.bronze.fetch_breweries_metadata", return_value={"total": 2}
    ) as mock_fetch_meta:
        extract_to_bronze(tmp_path, ds="2026-03-01", run_id="run_1")

    assert mock_fetch_rows.call_count == 1
    assert mock_fetch_meta.call_count == 1
