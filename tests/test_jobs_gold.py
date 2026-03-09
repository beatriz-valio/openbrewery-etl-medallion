import pytest
import pandas as pd
from src.jobs.gold import silver_to_gold
from unittest.mock import patch, MagicMock
from pathlib import Path


def test_silver_to_gold():
    base_path = "mock_base_path"
    ds = "2026-03-09"
    run_id = "test_run"

    mock_silver_dir = "mock_silver_dir"
    mock_gold_dir = "mock_gold_dir"

    with patch(
        "src.jobs.gold.silver_breweries_dir", return_value=mock_silver_dir
    ), patch("src.jobs.gold.gold_breweries_run_dir", return_value=mock_gold_dir), patch(
        "pathlib.Path.rglob", return_value=[Path("a.parquet"), Path("b.parquet")]
    ), patch(
        "pandas.read_parquet"
    ) as mock_read_parquet, patch(
        "pandas.DataFrame.to_parquet"
    ) as mock_to_parquet:

        mock_df = pd.DataFrame(
            {
                "country": ["US", "CA", "US"],
                "state_province": ["CA", "ON", "NY"],
                "brewery_type": ["micro", "nano", "brewpub"],
                "brewery_count": [10, 5, 15],
            }
        )

        mock_read_parquet.side_effect = [mock_df, mock_df]

        silver_to_gold(base_path, ds, run_id)

        assert mock_read_parquet.call_count == 2
        mock_to_parquet.assert_called_once()

        output_call = str(mock_to_parquet.call_args[0][0]).replace("\\", "/")
        assert "mock_gold_dir/result.parquet" in output_call


def test_silver_to_gold_no_files():
    base_path = "mock_base_path"
    ds = "2026-03-09"
    run_id = "test_run"

    mock_silver_dir = "mock_silver_dir"

    with patch(
        "src.jobs.gold.silver_breweries_dir", return_value=mock_silver_dir
    ), patch("pathlib.Path.rglob", return_value=[]):

        with pytest.raises(FileNotFoundError, match="No silver parquet files found"):
            silver_to_gold(base_path, ds, run_id)


def test_silver_to_gold_missing_columns():
    base_path = "mock_base_path"
    ds = "2026-03-09"
    run_id = "test_run"

    mock_silver_dir = "mock_silver_dir"
    mock_gold_dir = "mock_gold_dir"

    with patch(
        "src.jobs.gold.silver_breweries_dir", return_value=mock_silver_dir
    ), patch("src.jobs.gold.gold_breweries_run_dir", return_value=mock_gold_dir), patch(
        "pathlib.Path.rglob", return_value=[Path("p1.parquet")]
    ), patch(
        "pandas.read_parquet"
    ) as mock_read_parquet:

        mock_df = pd.DataFrame({"unexpected_column": [1, 2, 3]})
        mock_read_parquet.return_value = mock_df

        with pytest.raises(ValueError, match="Missing required columns"):
            silver_to_gold(base_path, ds, run_id)
