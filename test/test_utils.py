import pandas as pd
import pytest

from main.utils import capitalize_columns, categorize_columns, columns_to_datetime


def test_capitalize_columns():
    # Create a sample DataFrame
    df = pd.DataFrame({'names': ['alice in wonderland', 'bob marley', 'charlie and the chocolate factory', '']})

    # Apply the function
    capitalize_columns(df, 'names')

    # Check the results
    expected = pd.DataFrame({'names': ['Alice In Wonderland', 'Bob Marley', 'Charlie And The Chocolate Factory', '']})
    pd.testing.assert_frame_equal(df, expected)

def test_categorize_columns():
    # Create a sample DataFrame
    df = pd.DataFrame({'status': ['pending', 'complete', 'pending', 'failed']})

    # Apply the function
    categorize_columns(df, 'status')

    # Check the results
    assert pd.api.types.is_categorical_dtype(df['status']), "Column 'status' should be categorical"
    assert set(df['status'].cat.categories) == {'pending', 'complete', 'failed'}, "Categories do not match expected values"

def test_columns_to_datetime():
    # Create a sample DataFrame
    df = pd.DataFrame({'date': ['2021-01-01', '2021-01-02', '2021-01-03']})

    # Column names to convert
    date_columns = ['date']

    # Apply the function
    columns_to_datetime(df, date_columns)

    # Check the results
    assert pd.api.types.is_datetime64_any_dtype(df['date']), "Column 'date' should be of datetime type"

# This allows the test suite to be run from the command line
if __name__ == "__main__":
    pytest.main()