import tempfile
import pytest
import yaml
import os
import pandas as pd
from unittest.mock import patch, MagicMock


from main.main import (
    load_config,
    arg_parser,
    load_data,
    write_bronze_layer,
    write_silver_layer,
    write_gold_layer,
)


@pytest.fixture
def sample_data_frames():
    return {
        "data_customers": pd.DataFrame(
            {"customer_id": ["c1", "c2"], "customer_city": ["new york", "los angeles"]}
        ),
        "data_sellers": pd.DataFrame(
            {"seller_id": ["s1", "s2"], "seller_city": ["chicago", "houston"]}
        ),
        "data_order_items": pd.DataFrame(
            {
                "order_id": ["o1", "o2"],
                "price": [100, 150],
                "freight_value": [10, 15],
                "shipping_limit_date": ["2021-01-01", "2021-01-02"],
                "order_item_id": [1, 2],
            }
        ),
        "data_order_reviews": pd.DataFrame(
            {"order_id": ["o1", "o2"], "review_score": [5, 4]}
        ),
        "data_products": pd.DataFrame(
            {
                "product_id": ["p1", "p2"],
                "product_category_name": ["electronics", "books"],
            }
        ),
        "data_product_category_name_translation": pd.DataFrame(
            {
                "product_category_name": ["electronics", "books"],
                "product_category_name_english": ["Electronics", "Books"],
            }
        ),
        "aggregated_order_payments": pd.DataFrame(
            {"order_id": ["o1", "o2"], "payment_value": [110, 165]}
        ),
        "data_order_payments": pd.DataFrame(
            {"order_id": ["o1", "o2"], "payment_value": [110, 165]}
        ),
    }


@pytest.fixture
def config_data():
    return {
        "files": ["file1.csv", "file2.csv"],
        "paths": {
            "input_folder": "/path/to/input",
            "output_folder": "/path/to/output",
            "bronze_layer": "bronze",
            "silver_layer": "silver",
            "gold_layer": "gold",
        },
        "partition_columns": ["product_category"],
    }


@pytest.fixture
def csv_data():
    return """col1,col2
1,2
3,4
"""


@pytest.fixture
def csv_file(csv_data):
    # Create a temporary file in the current directory
    with tempfile.NamedTemporaryFile(
        delete=False, mode="w+t", suffix=".csv", newline="", dir="."
    ) as temp:
        temp.write(csv_data)
    # Yield the filename only
    yield os.path.basename(temp.name)
    # Remove the file after the test
    os.remove(temp.name)


def test_load_config():
    """Create a temporary YAML file for testing, load it and check if it's the same when loaded with load_config"""
    config_data = {
        "mytest": {
            "first_test": "This is the test",
            "second_test": "This is the second test",
        },
    }

    # Path for a temporary config file
    temp_config_path = "dummy_for_test.yaml"

    # Write test data to the temporary YAML file
    with open(temp_config_path, "w") as file:
        yaml.dump(config_data, file)

    try:
        config = load_config(temp_config_path)
        assert config == config_data, "Loaded data is not the same"

    finally:
        os.remove(temp_config_path)


def test_load_config_missing_file():
    """check if function properly raises FileNotFoundError in case of missing yaml file"""
    with pytest.raises(FileNotFoundError):
        load_config("non_existent_file.yaml")


def test_arg_parser_default():
    """Test with no command line arguments (default values)"""
    with patch("sys.argv", ["main/main.py"]):
        args = arg_parser()
        assert args.config_file == "param_config.yaml"


def test_arg_parser_with_args():
    """Test with specific command line arguments"""
    with patch("sys.argv", ["main/main.py", "--config_file", "custom_config.yaml"]):
        args = arg_parser()
        assert args.config_file == "custom_config.yaml"


def test_arg_parser_help(capsys):
    """Tests if the help output works properly"""
    with pytest.raises(SystemExit), patch("sys.argv", ["main/main.py", "--help"]):
        arg_parser()
        captured = capsys.readouterr()
        assert "--config_file" in captured.out


def test_load_data(csv_file):
    # Use the current directory as the base path
    base_path = "."
    files_list = [csv_file, csv_file]  # Using the same temp file twice for simplicity

    # Call the actual function
    data_frames = load_data(base_path, files_list)

    # Assertions to check if data is loaded correctly
    expected_keys = {
        f"data_{os.path.splitext(csv_file)[0]}",
        f"data_{os.path.splitext(csv_file)[0]}",
    }
    assert (
        set(data_frames.keys()) == expected_keys
    ), "DataFrame keys do not match expected names"
    assert all(
        isinstance(df, pd.DataFrame) for df in data_frames.values()
    ), "Not all values are DataFrames"


@patch("builtins.open")
@patch("yaml.safe_load")
def test_load_config(mock_safe_load, mock_open, config_data):
    mock_safe_load.return_value = config_data
    config = load_config("dummy_path")
    mock_open.assert_called_once_with("dummy_path", "r")
    assert config == config_data


@patch("os.makedirs")
@patch("pandas.DataFrame.to_parquet")
def test_write_bronze_layer(mock_to_parquet, mock_makedirs):
    data_frames = {"data_test": pd.DataFrame({"col1": [1, 2]})}
    write_bronze_layer(data_frames, "/fake/path")
    mock_makedirs.assert_called_once_with("/fake/path", exist_ok=True)
    mock_to_parquet.assert_called_once()


# Test write_silver_layer function
@patch("os.makedirs")
@patch("pandas.DataFrame.to_parquet")
def test_write_silver_layer(mock_to_parquet, mock_makedirs, sample_data_frames):
    write_silver_layer(sample_data_frames, "/fake/path")
    assert mock_makedirs.called
    assert mock_to_parquet.call_count == len(sample_data_frames)


@pytest.fixture
def sample_data_frame():
    # Create a sample DataFrame with necessary columns for partitioning
    return pd.DataFrame(
        {
            "product_id": ["p1", "p2"],
            "english_category_name": ["electronics", "clothing"],
            "sales": [100, 150],
        }
    )


@patch("os.makedirs")
@patch("pandas.DataFrame.to_parquet")
def test_write_gold_layer(mock_to_parquet, mock_makedirs, sample_data_frame):
    # Define the path and partition column
    path = "/fake/path"
    partition_on = ["english_category_name"]

    # Call the function with the sample DataFrame
    write_gold_layer(sample_data_frame, path, partition_on)

    # Check if the directory creation method works
    mock_makedirs.assert_called_once_with(path, exist_ok=True)

    # Check if the to_parquet method is working properly
    file_path = f"{path}/big_table_gold.parquet"
    mock_to_parquet.assert_called_once_with(file_path, partition_cols=partition_on)


if __name__ == "__main__":
    pytest.main()
