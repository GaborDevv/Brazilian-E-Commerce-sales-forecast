import pandas as pd
import pytest

from main.utils import (
    capitalize_columns,
    categorize_columns,
    columns_to_datetime,
    column_dropper,
)


@pytest.fixture
def sample_data():
    data1 = pd.DataFrame(
        {
            "name": ["alice", "bob", "charlie"],
            "city": ["new york", "los angeles", "chicago"],
        }
    )
    data2 = pd.DataFrame(
        {
            "date": ["2021-01-01", "2021-02-01"],
            "datetime": ["2021-01-01 10:00", "2021-02-01 20:00"],
        }
    )

    data3 = df = pd.DataFrame(
        {
            "names": [
                "alice in wonderland",
                "bob marley",
                "charlie and the chocolate factory",
                "",
            ],
            "places": ["here", "there", "everywhere", "nowhere"],
        }
    )

    return data1, data2, data3


def test_capitalize_columns(sample_data):
    data1, _, data3 = sample_data
    capitalize_columns(data1, data3, col1="city", col2="names")
    assert data1["city"].equals(pd.Series(["New York", "Los Angeles", "Chicago"]))
    assert data3["names"].equals(
        pd.Series(
            [
                "Alice In Wonderland",
                "Bob Marley",
                "Charlie And The Chocolate Factory",
                "",
            ]
        )
    )


def test_categorize_columns(sample_data):
    data1, _, data3 = sample_data
    categorize_columns(data1, data3, col1=["city"], col2=["places"])
    assert pd.api.types.is_categorical_dtype(
        data1["city"]
    ), "Column 'city' should be categorical"
    assert set(data3["places"].cat.categories) == {
        "here",
        "there",
        "everywhere",
        "nowhere",
    }, "Categories do not match expected values"


def test_columns_to_datetime(sample_data):
    _, data2, _ = sample_data
    columns_to_datetime(data2, col1=["date", "datetime"])
    assert pd.api.types.is_datetime64_any_dtype(
        data2["date"]
    ), "Column 'date' should be of datetime type"
    assert pd.api.types.is_datetime64_any_dtype(
        data2["datetime"]
    ), "Column 'date' should be of datetime type"


def test_column_dropper(sample_data):
    data1, data2, data3 = sample_data
    column_dropper(
        data1, data2, data3, col1=["name"], col2=["datetime"], col3=["places"]
    )
    assert "name" not in data1.columns
    assert "datetime" not in data2.columns
    assert "places" not in data3.columns


if __name__ == "__main__":
    pytest.main()
