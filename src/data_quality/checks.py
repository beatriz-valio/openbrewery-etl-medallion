import logging
from pathlib import Path
from typing import Optional

import pandas as pd

from src.paths.lake import gold_breweries_result_path, silver_breweries_dir

logger = logging.getLogger(__name__)


def run_dq_checks(base_path: str, ds: str, run_id: str) -> None:
    gold_path = gold_breweries_result_path(base_path, ds, run_id)
    df = pd.read_parquet(gold_path)

    logger.info("Data Quality Checks: gold_path=%s rows=%s", gold_path, len(df))

    # hard checks
    assert len(df) > 0, "Gold is empty"

    required = {
        "country",
        "state_province",
        "brewery_type",
        "brewery_count",
        "ingestion_date",
        "run_id",
    }
    missing = required - set(df.columns)
    assert not missing, f"Columns missing in gold: {missing}"

    assert (df["brewery_count"] >= 0).all(), "brewery_count is invalid"
    assert (
        df["ingestion_date"] == ds
    ).all(), "ingestion_date does not match current run"
    assert (df["run_id"] == run_id).all(), "run_id does not match current run"

    dupes = df.duplicated(subset=["country", "state_province", "brewery_type"]).sum()
    assert dupes == 0, f"Duplicated gold groups found: {dupes}"

    silver_root = Path(silver_breweries_dir(base_path, ds, run_id))
    silver_files = sorted(silver_root.rglob("*.parquet"))
    silver_df = pd.concat([pd.read_parquet(f) for f in silver_files], ignore_index=True)
    assert int(df["brewery_count"].sum()) == len(silver_df), (
        f"Gold count mismatch: gold_sum={int(df['brewery_count'].sum())} "
        f"silver_rows={len(silver_df)}"
    )

    max_count = int(df["brewery_count"].max())
    assert (
        max_count < 10_000
    ), f"brewery_count is too high for a single partition: {max_count}"

    # soft checks
    unknown_ratio = (
        (df["country"] == "unknown")
        | (df["state_province"] == "unknown")
        | (df["brewery_type"] == "unknown")
    ).mean()

    previous_gold = _find_previous_gold_run(base_path, ds, run_id)
    if previous_gold:
        prev_df = pd.read_parquet(previous_gold)

        current_total = int(df["brewery_count"].sum())
        previous_total = int(prev_df["brewery_count"].sum())
        total_change = _percentual_change(current_total, previous_total)

        current_groups = len(df)
        previous_groups = len(prev_df)
        groups_change = _percentual_change(current_groups, previous_groups)

        prev_unknown_ratio = (
            (prev_df["country"] == "unknown")
            | (prev_df["state_province"] == "unknown")
            | (prev_df["brewery_type"] == "unknown")
        ).mean()

        if total_change > 0.10:
            raise AssertionError(
                f"Total brewery volume changed more than 10%: "
                f"current={current_total}, previous={previous_total}, pct_change={total_change:.2%}"
            )
        elif total_change > 0.05:
            logger.warning(
                "Total brewery volume changed more than 5%%: current=%s previous=%s pct_change=%.2f%%",
                current_total,
                previous_total,
                total_change * 100,
            )

        if groups_change > 0.10:
            raise AssertionError(
                f"Gold group count changed more than 10%: "
                f"current={current_groups}, previous={previous_groups}, pct_change={groups_change:.2%}"
            )
        elif groups_change > 0.05:
            logger.warning(
                "Gold group count changed more than 5%%: current=%s previous=%s pct_change=%.2f%%",
                current_groups,
                previous_groups,
                groups_change * 100,
            )

        if unknown_ratio - prev_unknown_ratio > 0.05:
            logger.warning(
                "Unknown ratio increased materially: current=%.2f%% previous=%.2f%%",
                unknown_ratio * 100,
                prev_unknown_ratio * 100,
            )

    current_types = set(df["brewery_type"].dropna().astype(str).unique())
    logger.info("Observed brewery types in gold: %s", sorted(current_types))


def _find_previous_gold_run(
    base_path: str, current_ds: str, current_run_id: str
) -> Optional[Path]:
    gold_root = Path(base_path) / "gold" / "breweries_by_type_location"
    candidates = sorted(gold_root.glob("ingestion_date=*/run_id=*/result.parquet"))

    previous = []
    current_path = gold_breweries_result_path(base_path, current_ds, current_run_id)

    for path in candidates:
        if path != current_path:
            previous.append(path)

    return previous[-1] if previous else None


def _percentual_change(current: float, previous: float) -> float:
    if previous == 0:
        return 0.0 if current == 0 else 1.0
    return abs(current - previous) / previous
