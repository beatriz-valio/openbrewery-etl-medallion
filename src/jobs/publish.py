import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from src.paths.lake import gold_breweries_result_path, gold_breweries_run_dir

logger = logging.getLogger(__name__)


def publish_gold(base_path: str, ds: str, run_id: str) -> None:
    logger.info(
        "Starting Gold publication: base_path=%s ds=%s run_id=%s", base_path, ds, run_id
    )

    run_dir = gold_breweries_run_dir(base_path, ds, run_id)
    gold_file = gold_breweries_result_path(base_path, ds, run_id)

    if not gold_file.exists():
        raise FileNotFoundError(f"Gold file not found: {gold_file}")

    success_path = run_dir / "_SUCCESS"
    success_path.write_text("", encoding="utf-8")

    gold_root = run_dir.parent.parent
    latest_path = gold_root / "_LATEST.json"
    payload = {
        "dataset": "breweries_by_type_location",
        "ingestion_date": ds,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "gold_path": str(gold_file),
        "published_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    logger.info("Saving latest published gold metadata: %s", payload)
    _save_latest_path(latest_path, payload)

    logger.info("Published Gold: latest=%s | success=%s", latest_path, success_path)


def _save_latest_path(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)
