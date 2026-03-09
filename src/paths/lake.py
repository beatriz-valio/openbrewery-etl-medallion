from pathlib import Path


def bronze_breweries_dir(base_path: str, ds: str, run_id: str) -> Path:
    return (
        Path(base_path)
        / "bronze"
        / "breweries"
        / f"ingestion_date={ds}"
        / f"run_id={_run_id_treated(run_id)}"
    )


def bronze_breweries_file(base_path: str, ds: str, run_id: str) -> Path:
    return bronze_breweries_dir(base_path, ds, run_id) / "breweries.jsonl"


def silver_breweries_dir(base_path: str, ds: str, run_id: str) -> Path:
    return (
        Path(base_path)
        / "silver"
        / "breweries"
        / f"ingestion_date={ds}"
        / f"run_id={_run_id_treated(run_id)}"
    )


def gold_breweries_run_dir(base_path: str, ds: str, run_id: str) -> Path:
    return (
        Path(base_path)
        / "gold"
        / "breweries_by_type_location"
        / f"ingestion_date={ds}"
        / f"run_id={_run_id_treated(run_id)}"
    )


def gold_breweries_result_path(base_path: str, ds: str, run_id: str) -> Path:
    return gold_breweries_run_dir(base_path, ds, run_id) / "result.parquet"


def _run_id_treated(run_id: str) -> str:
    return run_id.replace(":", "_").replace("+", "_")
