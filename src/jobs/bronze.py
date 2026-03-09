import json
import logging
import os
from pathlib import Path

from src.clients.openbrewery import fetch_all_breweries, fetch_breweries_metadata
from src.paths.lake import bronze_breweries_dir

logger = logging.getLogger(__name__)


def extract_to_bronze(base_path: Path, ds: str, run_id: str) -> None:
    logger.info(
        "Starting extraction to Bronze: base_path=%s ds=%s run_id=%s",
        base_path,
        ds,
        run_id,
    )

    try:
        out_dir = _create_directories(base_path, ds, run_id)

        data_path = out_dir / "breweries.jsonl"
        manifest_path = out_dir / "manifest.json"

        total = 0
        for batch in fetch_all_breweries():
            with data_path.open("a", encoding="utf-8") as f:
                for row in batch:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")
                    total += 1

        metadata = fetch_breweries_metadata()
        expected_total = _check_expected_total(metadata)
        if expected_total is not None and total != expected_total:
            logger.warning(
                "Total records fetched does not match metadata total. Records=(%s) | Metadata Total=(%s)",
                total,
                expected_total,
            )

        manifest = {
            "ingestion_date": ds,
            "run_id": run_id,
            "records": total,
            "source_expected_total": expected_total,
        }
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        logger.info(
            "Bronze: path=%s records=%s ingestion_date=%s run_id=%s",
            data_path,
            total,
            ds,
            run_id,
        )
    except Exception as e:
        logger.error("Failed to extract to bronze: %s", e)
        raise


def _create_directories(base_path: Path, ds: str, run_id: str) -> Path:
    base_path_dir = Path(base_path)
    if not base_path_dir.exists():
        logger.info("Base path does not exist. Creating: %s", base_path_dir)
        base_path_dir.mkdir(parents=True, exist_ok=True)
    elif not os.access(base_path_dir, os.W_OK):
        raise PermissionError(f"Base path is not writable: {base_path_dir}")

    out_dir = bronze_breweries_dir(base_path, ds, run_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _check_expected_total(metadata: dict) -> None:
    try:
        return int(metadata["total"])
    except (ValueError, TypeError) as e:
        logger.error(
            "Invalid metadata total value: %s. Error: %s.", metadata["total"], e
        )
