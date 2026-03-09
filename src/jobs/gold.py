import logging
import shutil
from pathlib import Path

import pandas as pd

from src.paths.lake import gold_breweries_run_dir, silver_breweries_dir

logger = logging.getLogger(__name__)


DIMENSIONS = ["country", "state_province", "brewery_type"]


def silver_to_gold(base_path: str, ds: str, run_id: str) -> None:
    logger.info(
        "Starting Silver to Gold transformation: base_path=%s ds=%s run_id=%s",
        base_path,
        ds,
        run_id,
    )

    silver_root = Path(silver_breweries_dir(base_path, ds, run_id))
    parquet_files = sorted(silver_root.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No silver parquet files found under {silver_root}")

    logger.info(
        "Reading Silver parquet files: path=%s files=%s",
        silver_root,
        len(parquet_files),
    )

    frames = [pd.read_parquet(file) for file in parquet_files]
    df = pd.concat(frames, ignore_index=True)

    logger.info("Silver rows loaded for Gold: rows=%s", len(df))

    required = set(DIMENSIONS)
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns in Silver for Gold aggregation: {missing}"
        )

    for col in DIMENSIONS:
        df = _normalize_text_col(df, col)

    logger.info("Aggregating data for Gold")

    agg = (
        df.groupby(DIMENSIONS, dropna=False)
        .size()
        .reset_index(name="brewery_count")
        .sort_values(DIMENSIONS)
        .reset_index(drop=True)
    )

    agg["brewery_count"] = agg["brewery_count"].astype("int64")
    agg["ingestion_date"] = ds
    agg["run_id"] = run_id

    agg = agg[
        [
            "country",
            "state_province",
            "brewery_type",
            "brewery_count",
            "ingestion_date",
            "run_id",
        ]
    ]

    total_groups = len(agg)
    total_breweries = int(agg["brewery_count"].sum())

    gold_dir = Path(gold_breweries_run_dir(base_path, ds, run_id))
    if gold_dir.exists():
        shutil.rmtree(gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)

    output_path = gold_dir / "result.parquet"
    logger.info("Writing Gold parquet file: path=%s rows=%s", output_path, len(agg))

    agg.to_parquet(
        output_path,
        index=False,
        engine="pyarrow",
    )

    logger.info(
        "Gold: path=%s groups=%s total_breweries=%s ingestion_date=%s run_id=%s",
        gold_dir,
        total_groups,
        total_breweries,
        ds,
        run_id,
    )


def _normalize_text_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        return df

    s = df[col].astype("string").str.strip()
    df[col] = s.mask(
        s.isna() | (s == "") | (s.str.lower() == "none"),
        "unknown",
    )
    return df
