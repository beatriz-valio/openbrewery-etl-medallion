from pathlib import Path

from src.paths.lake import (
    bronze_breweries_dir,
    bronze_breweries_file,
    gold_breweries_result_path,
    gold_breweries_run_dir,
    silver_breweries_dir,
)


def test_bronze_paths_are_built_consistently():
    base_path = Path("/lake")
    ds = "2026-03-08"
    run_id = "manual__2026-03-08T22:53:53+00:00"

    bronze_dir = bronze_breweries_dir(base_path, ds, run_id)
    bronze_file = bronze_breweries_file(base_path, ds, run_id)

    assert bronze_dir == (
        base_path
        / "bronze"
        / "breweries"
        / "ingestion_date=2026-03-08"
        / "run_id=manual__2026-03-08T22_53_53_00_00"
    )
    assert bronze_file == bronze_dir / "breweries.jsonl"


def test_silver_and_gold_paths_are_built_consistently():
    base_path = Path("/lake")
    ds = "2026-03-08"
    run_id = "manual__2026-03-08T22:53:53+00:00"

    silver_dir = silver_breweries_dir(base_path, ds, run_id)
    gold_dir = gold_breweries_run_dir(base_path, ds, run_id)
    gold_result = gold_breweries_result_path(base_path, ds, run_id)

    assert silver_dir == (
        base_path
        / "silver"
        / "breweries"
        / "ingestion_date=2026-03-08"
        / "run_id=manual__2026-03-08T22_53_53_00_00"
    )
    assert gold_dir == (
        base_path
        / "gold"
        / "breweries_by_type_location"
        / "ingestion_date=2026-03-08"
        / "run_id=manual__2026-03-08T22_53_53_00_00"
    )
    assert gold_result == gold_dir / "result.parquet"
