import pandas as pd

# Make the strings in the specified columns Title cased
def capitalize_columns(dataframe,col_name : str):
    dataframe[col_name] = dataframe[col_name].str.title()

# Make the data type of specified columns categorical
def categorize_columns(dataframe, column : str):
    status_categories_list = dataframe[column].dropna().unique().tolist()
    dataframe[column] = pd.Categorical(dataframe[column], categories=status_categories_list, ordered=False)


# Change the given columns' types to datetime, takes a df and a list as parameters
def columns_to_datetime(dataframe, date_columns):
    # Convert each column in the list to datetime
    for column in date_columns:
        dataframe[column] = pd.to_datetime(dataframe[column])



