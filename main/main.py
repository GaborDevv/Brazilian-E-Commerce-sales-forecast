from functools import reduce
import pandas as pd
import os
import pyarrow

from utils import categorize_columns, capitalize_columns, columns_to_datetime


# take a list as param and iterate through it while creating pandas dataframes and putting them into a dictionary
def load_data(files_list):
    data_frames = {}
    for file_path in files_list:
        name = file_path.split('/')[-1].replace('.csv', '').replace('olist_', '').replace('_dataset', '')
        var_name = f'data_{name}'
        data_frames[var_name] = pd.read_csv(file_path)
        print(f"Loaded new DataFrame: {var_name}")
    return data_frames


def fixing_schemas(dataframes):
    # parse date columns
    columns_to_datetime(dataframes['data_orders'], [
        'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date',
        'order_delivered_customer_date', 'order_estimated_delivery_date'
    ])
    columns_to_datetime(dataframes['data_order_items'], ['shipping_limit_date'])
    columns_to_datetime(dataframes['data_order_reviews'], ['review_creation_date', 'review_answer_timestamp'])
    columns_to_datetime(dataframes['data_order_items'], ['shipping_limit_date'])
    categorize_columns(dataframes['data_products'], 'product_category_name')
    categorize_columns(dataframes['data_product_category_name_translation'], 'product_category_name')
    categorize_columns(dataframes['data_product_category_name_translation'], 'product_category_name_english')


def write_bronze_layer(dict_of_dataframes, path):
    os.makedirs(path, exist_ok=True)  # Ensure the directory exists
    for key, dataframe in dict_of_dataframes.items():
        try:
            dataframe.to_excel(f'{path}/{key}_bronze.csv')
            print(f"DataFrame {key} written to {path}")
        except Exception as e:
            print(f"Failed to write {key} due to {e}")


def clean_data(data_frames):
    # capitalize city names
    capitalize_columns(data_frames['data_customers'], 'customer_city')
    capitalize_columns(data_frames['data_sellers'], 'seller_city')
    # remove unnecessary columns (we are building table for sales forecasts)
    data_frames['data_order_items'].drop('shipping_limit_date', axis=1, inplace=True)
    data_frames['data_order_payments'].drop(['payment_type', 'payment_sequential', 'payment_installments'], axis=1,
                                            inplace=True)
    data_frames['data_order_reviews'].drop(
        ['review_comment_title', 'review_comment_message', 'review_answer_timestamp'], axis=1, inplace=True)
    data_frames['data_orders'].drop(['order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date'], axis=1, inplace=True)


def write_silver_layer(dict_of_dataframes, path):
    os.makedirs(path, exist_ok=True)  # Ensure the directory exists
    for key, dataframe in dict_of_dataframes.items():
        try:
            dataframe.to_excel(f'{path}/{key}_silver.csv')
            print(f"DataFrame {key} written to {path}")
        except Exception as e:
            print(f"Failed to write {key} due to {e}")


# aggregate data with duplicate ids
def aggregate_data(data_frames):
    data_frames['aggregated_reviews'] = (data_frames['data_order_reviews'].groupby('order_id')
                                         .agg({'review_score': 'mean'}))
    data_frames['aggregated_order_payments'] = (data_frames['data_order_payments'].groupby('order_id')
                                                .agg(total_payment=('payment_value', 'sum')))
    data_frames['aggregated_order_items'] = (data_frames['data_order_items'].groupby('order_id')
    .agg({
        'price': 'sum',
        'freight_value': 'sum',
        'order_item_id': 'max',
    }).rename(columns={
        'price': 'total_order_value',
        'freight_value': 'total_freight_value',
        'order_item_id': 'number_of_items_ordered'
    }))


# merge the necessary tables into one, categorize columns
def merge_data(data_frames):
    products = pd.merge(
        data_frames['data_products'], data_frames['data_product_category_name_translation'],
        on='product_category_name', how='left'
    ).rename(columns={
        'product_category_name': 'original_category_name',
        'product_category_name_english': 'english_category_name'
    })[['product_id', 'original_category_name', 'english_category_name']]

    # join multiple tables on common key column
    orders_assist_list = [data_frames['data_orders'],
                          data_frames['data_order_items'],
                          data_frames['data_order_payments'],
                          data_frames['aggregated_order_payments'],
                          data_frames['aggregated_order_items'],
                          data_frames['aggregated_reviews']]

    orders_df = reduce(lambda left, right: pd.merge(left, right, on='order_id', how='inner'), orders_assist_list)
    orders_with_sellers_df = pd.merge(orders_df, data_frames['data_sellers'], on='seller_id', how='left')

    with_customers_df = pd.merge(orders_with_sellers_df, data_frames['data_customers'], on='customer_id', how='left')

    big_table = pd.merge(with_customers_df, products, on='product_id', how='inner').drop(
        ['order_status', 'customer_id', 'customer_unique_id'], axis=1)
    return big_table


def write_gold_layer(data_frame, path):
    # Ensure the directory exists
    os.makedirs(path, exist_ok=True)
    file_path = f'{path}/big_table_gold.parquet'
    # Write the DataFrame to a Parquet file, partitioned by 'english_category_name'
    try:
        data_frame.to_parquet(file_path, partition_cols=['english_category_name'])
        print(f"DataFrame written to {file_path}")
    except Exception as e:
        print(f"Failed to write {data_frame} due to {e}")


def main():
    files_list = [
        '../raw_data/olist_customers_dataset.csv',
        '../raw_data/olist_order_items_dataset.csv',
        '../raw_data/olist_order_payments_dataset.csv',
        '../raw_data/olist_orders_dataset.csv',
        '../raw_data/olist_products_dataset.csv',
        '../raw_data/olist_sellers_dataset.csv',
        '../raw_data/product_category_name_translation.csv',
        '../raw_data/olist_order_reviews_dataset.csv'
    ]
    path = '../bronze_layer'
    data_frames = load_data(files_list)
    fixing_schemas(data_frames)
    write_bronze_layer(data_frames, path)
    clean_data(data_frames)
    silver_path = '../silver_layer'
    write_silver_layer(data_frames, silver_path)
    aggregate_data(data_frames)
    big_table = merge_data(data_frames)

    print(big_table.dtypes)

    gold_path = '../gold_layer'

    write_gold_layer(big_table, gold_path)


if __name__ == '__main__':
    main()
