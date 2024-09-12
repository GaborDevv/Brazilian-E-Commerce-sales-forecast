import pandas as pd


def capitalize_columns(*dataframes, **column_names):
    """Make the strings in the specified columns Title cased
    Parameters:
    - *dataframes: Variable number of DataFrame arguments.
    - **column_lists: Keyword arguments where keys are 'columns1', 'columns2', etc.,
                      corresponding to each DataFrame.
    """
    if len(dataframes) != len(column_names):
        raise ValueError(
            "The number of DataFrames does not match the number of column names provided, in capitalize_columns function."
        )
    else:
        for i, dataframe in enumerate(dataframes, start=1):
            column_key = f"col{i}"
            if column_key in column_names:
                # Apply title case to the specified column
                column_to_capitalize = column_names[column_key]
                if column_to_capitalize in dataframe.columns:
                    dataframe[column_to_capitalize] = dataframe[
                        column_to_capitalize
                    ].str.title()
                else:
                    raise ValueError(
                        f"Column {column_to_capitalize} not found in DataFrame {i}"
                    )
            else:
                raise ValueError(f"No column name provided for DataFrame {i}")


def categorize_columns(*dataframes, **column_names):
    """Make the data type of specified columns categorical
    Parameters:
    - *dataframes: Variable number of DataFrame arguments.
    - **column_lists: Keyword arguments where keys are 'columns1', 'columns2', etc.,
                      corresponding to each DataFrame.
    """
    if len(dataframes) != len(column_names):
        raise ValueError(
            "The number of DataFrames does not match the number of column names provided, in capitalize_columns function."
        )
    else:
        for i, dataframe in enumerate(dataframes, start=1):
            column_key = f"col{i}"
            if column_key in column_names:
                for column_name in column_names[column_key]:
                    if column_name in dataframe.columns:
                        # Create a list of unique values, including NaN vals
                        status_categories_list = (
                            dataframe[column_name]
                            .astype("category")
                            .cat.categories.tolist()
                        )
                        # Convert the column to categorical
                        dataframe[column_name] = pd.Categorical(
                            dataframe[column_name],
                            categories=status_categories_list,
                            ordered=False,
                        )
            else:
                raise ValueError(f"Column {column_key} not found in DataFrame {i}")


def columns_to_datetime(*dataframes, **date_column_names):
    """Change the given columns' types to datetime
    Parameters:
    - *dataframes: Variable number of DataFrame arguments.
    - **column_lists: Keyword arguments where keys are 'columns1', 'columns2', etc.,
                      corresponding to each DataFrame.
    """
    if len(dataframes) != len(date_column_names):
        raise ValueError(
            "The number of DataFrames does not match the number of date column groups provided."
        )
    else:
        for i, dataframe in enumerate(dataframes, start=1):
            column_key = f"col{i}"
            if column_key in date_column_names:
                for column_name in date_column_names[column_key]:
                    if column_name in dataframe.columns:
                        dataframe[column_name] = pd.to_datetime(dataframe[column_name])

                        dataframe[column_name] = dataframe[column_name].astype(
                            "datetime64[us]"
                        )
                    else:
                        raise ValueError(
                            f"Column {column_name} not found in DataFrame {i}"
                        )
            else:
                raise ValueError(
                    f"Date column group {column_key} not provided for DataFrame {i}"
                )


def column_dropper(*dataframes, **column_lists):
    """
    Applies a function to multiple DataFrames using corresponding lists of columns.

    Parameters:
    - *dataframes: Variable number of DataFrame arguments.
    - **column_lists: Keyword arguments where keys are 'columns1', 'columns2', etc.,
                      corresponding to each DataFrame.
    """
    if len(dataframes) != len(column_lists):
        raise ValueError(
            "The number of DataFrames does not match the number of column lists provided."
        )
    else:
        for i, dataframe in enumerate(dataframes, start=1):
            column_key = f"col{i}"
            if column_key in column_lists:
                # Call the function with the current DataFrame and its column list
                dataframe.drop(column_lists[column_key], axis=1, inplace=True)
            else:
                raise ValueError(f"No column list provided for DataFrame {i}")
