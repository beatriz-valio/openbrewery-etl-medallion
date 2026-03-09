import json

import pandas as pd

from src.jobs.publish import publish_gold


def test_publish_gold_creates_latest_and_sucess(tmp_path):
    ds = "2026-03-08"
    run_id = "manual__2026-03-08T22:53:53+00:00"

    gold_dir = (
        tmp_path
        / "gold"
        / "breweries_by_type_location"
        / f"ingestion_date={ds}"
        / "run_id=manual__2026-03-08T22_53_53_00_00"
    )
    gold_dir.mkdir(parents=True, exist_ok=True)
    gold_file = gold_dir / "result.parquet"

    pd.DataFrame([{"country": "US", "brewery_count": 1}]).to_parquet(
        gold_file, index=False
    )
    publish_gold(str(tmp_path), ds=ds, run_id=run_id)

    latest_path = tmp_path / "gold" / "breweries_by_type_location" / "_LATEST.json"
    success_path = gold_dir / "_SUCCESS"

    assert latest_path.exists()
    assert success_path.exists()

    payload = json.loads(latest_path.read_text(encoding="utf-8"))

    assert payload["dataset"] == "breweries_by_type_location"
    assert payload["ingestion_date"] == ds
    assert payload["run_id"] == run_id
    assert payload["run_dir"] == str(gold_dir)
    assert payload["gold_path"] == str(gold_file)
    assert "published_at_utc" in payload
