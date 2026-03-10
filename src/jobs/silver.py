import logging
import unicodedata
import shutil
from pathlib import Path

import pandas as pd

from src.paths.lake import bronze_breweries_file, silver_breweries_dir

logger = logging.getLogger(__name__)


TEXT_COLUMNS = ["country", "state_province", "city", "brewery_type"]
NUMERIC_COLUMNS = ["latitude", "longitude"]
KNOWN_STATE_FIXES = {
    "K�rnten": "Karnten",
    "Nieder�sterreich": "Niederosterreich",
}


def bronze_to_silver(base_path: str, ds: str, run_id: str) -> None:
    logger.info(
        "Starting Bronze to Silver transformation: base_path=%s ds=%s run_id=%s",
        base_path,
        ds,
        run_id,
    )

    bronze_file = bronze_breweries_file(base_path, ds, run_id)
    silver_root = Path(silver_breweries_dir(base_path, ds, run_id))
    df = pd.read_json(bronze_file, lines=True)
    logger.info(
        "Bronze data loaded: rows=%s ingestion_date=%s run_id=%s",
        len(df),
        ds,
        run_id,
    )

    df.columns = [str(col).strip().lower() for col in df.columns]
    logger.info("Columns normalized: columns=%s", df.columns.tolist())

    for col in TEXT_COLUMNS:
        df = _normalize_text_col(df, col)
    logger.info("Data normalization completed for text columns")

    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    logger.info("Latitude and longitude converted to numeric")

    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])
        logger.info("Duplicates dropped based on id column")

    logger.info("Adding ingestion_date and run_id columns")
    df["ingestion_date"] = ds
    df["run_id"] = run_id

    if silver_root.exists():
        shutil.rmtree(silver_root)
    silver_root.mkdir(parents=True, exist_ok=True)

    logger.info("Partitioning data by country and state_province")
    grouped = df.groupby(["country", "state_province"], dropna=False, sort=True)

    logger.info(
        "Writing Silver parquet files: path=%s partitions=%s", silver_root, len(grouped)
    )
    for (country, state_province), partition_df in grouped:
        partition_dir = (
            silver_root / f"country={country}" / f"state_province={state_province}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        partition_df.to_parquet(
            partition_dir / "part-00000.parquet",
            index=False,
            engine="pyarrow",
        )

    logger.info(
        "Silver: path=%s rows=%s ingestion_date=%s run_id=%s",
        silver_root,
        len(df),
        ds,
        run_id,
    )


def _normalize_text_col(df: pd.DataFrame, col: str) -> pd.DataFrame:
    if col not in df.columns:
        return df

    string = df[col].astype("string").str.strip()

    string = string.mask(
        string.isna() | (string == "") | (string.str.lower() == "none"),
        "unknown",
    )

    if col == "state_province":
        string = string.replace(KNOWN_STATE_FIXES)

    string = string.apply(
        lambda value: (
            "".join(
                char
                for char in unicodedata.normalize("NFKD", value)
                if not unicodedata.combining(char)
            )
            if pd.notna(value)
            else value
        )
    )

    string = string.str.replace(r'[\\/:*?"<>|]', "_", regex=True)

    df[col] = string
    return df
