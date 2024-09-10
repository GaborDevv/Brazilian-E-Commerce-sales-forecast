import pandas as pd
import pytest

from main.utils import capitalize_columns, categorize_columns, columns_to_datetime


def test_capitalize_columns():
    df = pd.DataFrame(
        {
            "names": [
                "alice in wonderland",
                "bob marley",
                "charlie and the chocolate factory",
                "",
            ]
        }
    )

    capitalize_columns(df, "names")

    expected = pd.DataFrame(
        {
            "names": [
                "Alice In Wonderland",
                "Bob Marley",
                "Charlie And The Chocolate Factory",
                "",
            ]
        }
    )
    pd.testing.assert_frame_equal(df, expected)


def test_categorize_columns():
    df = pd.DataFrame({"status": ["pending", "complete", "pending", "failed"]})

    categorize_columns(df, "status")

    assert pd.api.types.is_categorical_dtype(
        df["status"]
    ), "Column 'status' should be categorical"
    assert set(df["status"].cat.categories) == {
        "pending",
        "complete",
        "failed",
    }, "Categories do not match expected values"


def test_columns_to_datetime():
    df = pd.DataFrame({"date": ["2021-01-01", "2021-01-02", "2021-01-03"]})

    date_columns = ["date"]

    columns_to_datetime(df, date_columns)

    assert pd.api.types.is_datetime64_any_dtype(
        df["date"]
    ), "Column 'date' should be of datetime type"


if __name__ == "__main__":
    pytest.main()
