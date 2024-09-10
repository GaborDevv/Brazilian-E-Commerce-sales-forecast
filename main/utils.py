import pandas as pd


def capitalize_columns(dataframe, col_name: str):
    """Make the strings in the specified columns Title cased"""
    dataframe[col_name] = dataframe[col_name].str.title()


def categorize_columns(dataframe, column: str):
    """Make the data type of specified columns categorical"""
    status_categories_list = dataframe[column].dropna().unique().tolist()
    dataframe[column] = pd.Categorical(
        dataframe[column], categories=status_categories_list, ordered=False
    )


def columns_to_datetime(dataframe, date_columns):
    """Change the given columns' types to datetime, takes a df and a list as parameters"""
    for column in date_columns:
        dataframe[column] = pd.to_datetime(dataframe[column])
