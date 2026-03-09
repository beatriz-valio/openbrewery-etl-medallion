import pandas as pd
from src.jobs.silver import bronze_to_silver
from unittest.mock import patch
from pathlib import Path


def test_bronze_to_silver():
    base_path = "mock_base_path"
    ds = "2026-03-09"
    run_id = "test_run"

    mock_bronze_file = Path("mock_bronze_file.json")
    mock_silver_dir = Path("mock_silver_dir")

    with patch("src.jobs.silver.bronze_breweries_file", return_value=mock_bronze_file), patch("src.jobs.silver.silver_breweries_dir", return_value=mock_silver_dir), patch("pandas.read_json") as mock_read_json, patch("pandas.DataFrame.to_parquet") as mock_to_parquet:

        mock_df = pd.DataFrame({
            "id": [1, 2, 3],
            "country": ["US", "CA", "US"],
            "state_province": ["CA", "ON", "NY"],
            "city": ["Los Angeles", "Toronto", "New York"],
            "brewery_type": ["micro", "nano", "brewpub"],
            "latitude": [34.05, 43.7, 40.71],
            "longitude": [-118.25, -79.42, -74.0]
        })

        mock_read_json.return_value = mock_df

        bronze_to_silver(base_path, ds, run_id)

        mock_read_json.assert_called_once_with(mock_bronze_file, lines=True)
        assert mock_to_parquet.call_count == 3
