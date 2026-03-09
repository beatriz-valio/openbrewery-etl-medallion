from pathlib import Path

import pandas as pd
import pytest

from src.data_quality.checks import _find_previous_gold_run, run_dq_checks

GOLD_COLUMNS = [
    "country",
    "state_province",
    "brewery_type",
    "brewery_count",
    "ingestion_date",
    "run_id",
]


def _write_silver_rows(base_path: Path, ds: str, run_id: str, rows) -> Path:
    silver_root = (
        base_path / "silver" / "breweries" / f"ingestion_date={ds}" / f"run_id={run_id}"
    )
    silver_root.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(silver_root / "part-00000.parquet", index=False)
    return silver_root


def _write_gold_rows(base_path: Path, ds: str, run_id: str, rows) -> Path:
    gold_dir = (
        base_path
        / "gold"
        / "breweries_by_type_location"
        / f"ingestion_date={ds}"
        / f"run_id={run_id}"
    )
    gold_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(gold_dir / "result.parquet", index=False)
    return gold_dir / "result.parquet"


def _build_gold_from_silver_rows(ds: str, run_id: str, silver_rows):
    silver_df = pd.DataFrame(silver_rows)
    agg = (
        silver_df.groupby(["country", "state_province", "brewery_type"], dropna=False)
        .size()
        .reset_index(name="brewery_count")
    )
    agg["ingestion_date"] = ds
    agg["run_id"] = run_id
    return agg[GOLD_COLUMNS]


def test_run_dq_checks_passes_with_consistent_gold_and_silver(tmp_path):
    ds = "2026-03-08"
    run_id = "manual__2026-03-08T22_53_53_00_00"

    silver_rows = [
        {"country": "US", "state_province": "CA", "brewery_type": "micro", "id": 1},
        {"country": "US", "state_province": "CA", "brewery_type": "micro", "id": 2},
        {"country": "US", "state_province": "NY", "brewery_type": "regional", "id": 3},
    ]
    _write_silver_rows(tmp_path, ds, run_id, silver_rows)
    _write_gold_rows(
        tmp_path, ds, run_id, _build_gold_from_silver_rows(ds, run_id, silver_rows)
    )

    run_dq_checks(str(tmp_path), ds=ds, run_id=run_id)


def test_run_dq_checks_fails_when_required_columns_are_missing(tmp_path):
    ds = "2026-03-08"
    run_id = "run_1"

    _write_silver_rows(
        tmp_path,
        ds,
        run_id,
        [{"country": "US", "state_province": "CA", "brewery_type": "micro"}],
    )
    gold_dir = (
        tmp_path
        / "gold"
        / "breweries_by_type_location"
        / f"ingestion_date={ds}"
        / f"run_id={run_id}"
    )
    gold_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "country": "US",
                "state_province": "CA",
                "brewery_count": 1,
                "ingestion_date": ds,
                "run_id": run_id,
            }
        ]
    ).to_parquet(gold_dir / "result.parquet", index=False)

    with pytest.raises(AssertionError, match="Columns missing in gold"):
        run_dq_checks(str(tmp_path), ds=ds, run_id=run_id)


def test_run_dq_checks_fails_when_gold_total_does_not_match_silver(tmp_path):
    ds = "2026-03-08"
    run_id = "run_1"

    silver_rows = [
        {"country": "US", "state_province": "CA", "brewery_type": "micro"},
        {"country": "US", "state_province": "CA", "brewery_type": "micro"},
    ]
    _write_silver_rows(tmp_path, ds, run_id, silver_rows)
    _write_gold_rows(
        tmp_path,
        ds,
        run_id,
        [
            {
                "country": "US",
                "state_province": "CA",
                "brewery_type": "micro",
                "brewery_count": 1,
                "ingestion_date": ds,
                "run_id": run_id,
            }
        ],
    )

    with pytest.raises(AssertionError, match="Gold count mismatch"):
        run_dq_checks(str(tmp_path), ds=ds, run_id=run_id)


def test_run_dq_checks_fails_when_gold_groups_are_duplicated(tmp_path):
    ds = "2026-03-08"
    run_id = "run_1"

    silver_rows = [
        {"country": "US", "state_province": "CA", "brewery_type": "micro"},
        {"country": "US", "state_province": "CA", "brewery_type": "micro"},
    ]
    _write_silver_rows(tmp_path, ds, run_id, silver_rows)
    _write_gold_rows(
        tmp_path,
        ds,
        run_id,
        [
            {
                "country": "US",
                "state_province": "CA",
                "brewery_type": "micro",
                "brewery_count": 1,
                "ingestion_date": ds,
                "run_id": run_id,
            },
            {
                "country": "US",
                "state_province": "CA",
                "brewery_type": "micro",
                "brewery_count": 1,
                "ingestion_date": ds,
                "run_id": run_id,
            },
        ],
    )

    with pytest.raises(AssertionError, match="Duplicated gold groups found"):
        run_dq_checks(str(tmp_path), ds=ds, run_id=run_id)


def test_run_dq_checks_logs_warning_for_large_but_non_breaking_volume_change(
    tmp_path, caplog
):
    ds_prev = "2026-03-07"
    ds_curr = "2026-03-08"
    run_id_prev = "run_prev"
    run_id_curr = "run_curr"

    prev_rows = [
        {"country": "US", "state_province": "CA", "brewery_type": "micro"}
        for _ in range(100)
    ]
    curr_rows = [
        {"country": "US", "state_province": "CA", "brewery_type": "micro"}
        for _ in range(107)
    ]

    _write_silver_rows(tmp_path, ds_prev, run_id_prev, prev_rows)
    _write_gold_rows(
        tmp_path,
        ds_prev,
        run_id_prev,
        _build_gold_from_silver_rows(ds_prev, run_id_prev, prev_rows),
    )

    _write_silver_rows(tmp_path, ds_curr, run_id_curr, curr_rows)
    _write_gold_rows(
        tmp_path,
        ds_curr,
        run_id_curr,
        _build_gold_from_silver_rows(ds_curr, run_id_curr, curr_rows),
    )

    run_dq_checks(str(tmp_path), ds=ds_curr, run_id=run_id_curr)

    assert "Total brewery volume changed more than 5%" in caplog.text


def test_run_dq_checks_fails_for_large_volume_change(tmp_path):
    ds_prev = "2026-03-07"
    ds_curr = "2026-03-08"
    run_id_prev = "run_prev"
    run_id_curr = "run_curr"

    prev_rows = [
        {"country": "US", "state_province": "CA", "brewery_type": "micro"}
        for _ in range(100)
    ]
    curr_rows = [
        {"country": "US", "state_province": "CA", "brewery_type": "micro"}
        for _ in range(120)
    ]

    _write_silver_rows(tmp_path, ds_prev, run_id_prev, prev_rows)
    _write_gold_rows(
        tmp_path,
        ds_prev,
        run_id_prev,
        _build_gold_from_silver_rows(ds_prev, run_id_prev, prev_rows),
    )

    _write_silver_rows(tmp_path, ds_curr, run_id_curr, curr_rows)
    _write_gold_rows(
        tmp_path,
        ds_curr,
        run_id_curr,
        _build_gold_from_silver_rows(ds_curr, run_id_curr, curr_rows),
    )

    with pytest.raises(
        AssertionError, match="Total brewery volume changed more than 10%"
    ):
        run_dq_checks(str(tmp_path), ds=ds_curr, run_id=run_id_curr)


def test_find_previous_gold_run_returns_latest_previous_result(tmp_path):
    gold_root = tmp_path / "gold" / "breweries_by_type_location"
    prev_old = gold_root / "ingestion_date=2026-03-06" / "run_id=run_old"
    prev_new = gold_root / "ingestion_date=2026-03-07" / "run_id=run_prev"
    current = gold_root / "ingestion_date=2026-03-08" / "run_id=run_curr"

    for directory in [prev_old, prev_new, current]:
        directory.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "country": "US",
                    "state_province": "CA",
                    "brewery_type": "micro",
                    "brewery_count": 1,
                    "ingestion_date": directory.parent.name.split("=", 1)[1],
                    "run_id": directory.name.split("=", 1)[1],
                }
            ]
        ).to_parquet(directory / "result.parquet", index=False)

    previous = _find_previous_gold_run(str(tmp_path), "2026-03-08", "run_curr")

    assert previous == prev_new / "result.parquet"
