from functools import reduce
import pandas as pd

from utils import categorize_columns, capitalize_columns, columns_to_datetime

#take a list as param and iterate through it while creating pandas dataframes and putting them into a dictionary
def load_data(files_list):
    data_frames = {}
    for file_path in files_list:
        name = file_path.split('/')[-1].replace('.csv', '').replace('olist_', '').replace('_dataset', '')
        var_name = f'data_{name}'
        data_frames[var_name] = pd.read_csv(file_path)
        print(f"Loaded new DataFrame: {var_name}")
    return data_frames


def clean_data(data_frames):
    #parse date columns
    columns_to_datetime(data_frames['data_orders'], [
        'order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date',
        'order_delivered_customer_date', 'order_estimated_delivery_date'
    ])
    columns_to_datetime(data_frames['data_order_items'],['shipping_limit_date'])
    # capitalize city names
    capitalize_columns(data_frames['data_customers'], 'customer_city')
    capitalize_columns(data_frames['data_sellers'], 'seller_city')
    # remove unnecessary columns (we are building table for sales forecast not for delivery planning)
    data_frames['data_order_items'].drop('shipping_limit_date', axis=1, inplace=True)
    data_frames['data_order_payments'].drop(['payment_type', 'payment_sequential', 'payment_installments'], axis=1, inplace=True)


#aggregate data with duplicate ids
def aggregate_data(data_frames):
    order_payments = data_frames['data_order_payments'].groupby('order_id').agg(total_payment=('payment_value', 'sum'))
    order_items = data_frames['data_order_items'].groupby('order_id').agg({
        'price': 'sum',
        'freight_value': 'sum',
        'order_item_id': 'max',
        'product_id': lambda x: x.mode()[0] if not x.empty else None,
        'seller_id': lambda x: x.mode()[0] if not x.empty else None,
    }).rename(columns={
        'price': 'total_order_value',
        'freight_value': 'total_freight_value',
        'order_item_id': 'number_of_items_ordered'
    })
    return order_payments, order_items

#merge the necessary tables into one, categorize columns
def merge_data(data_frames, order_items, order_payments):
    products = pd.merge(
        data_frames['data_products'], data_frames['data_product_category_name_translation'],
        on='product_category_name', how='left'
    ).rename(columns={
        'product_category_name': 'original_category_name',
        'product_category_name_english': 'english_category_name'
    })[['product_id', 'original_category_name', 'english_category_name']]
    categorize_columns(products, 'original_category_name')
    categorize_columns(products, 'english_category_name')

    # join multiple tables on common key column
    orders_assist_list = [data_frames['data_orders'], order_items, order_payments]
    orders_df = reduce(lambda left, right: pd.merge(left, right, on='order_id', how='inner'), orders_assist_list)

    orders_with_sellers_df = pd.merge(orders_df, data_frames['data_sellers'], on='seller_id', how='left')

    with_customers_df = pd.merge(orders_with_sellers_df, data_frames['data_customers'], on='customer_id', how='left')

    big_table = pd.merge(with_customers_df, products, on='product_id', how='inner').drop(
        ['order_status', 'customer_id', 'customer_unique_id'], axis=1)
    return big_table

def main():
    files_list = [
        '../raw_data/olist_customers_dataset.csv',
        '../raw_data/olist_order_items_dataset.csv',
        '../raw_data/olist_order_payments_dataset.csv',
        '../raw_data/olist_orders_dataset.csv',
        '../raw_data/olist_products_dataset.csv',
        '../raw_data/olist_sellers_dataset.csv',
        '../raw_data/product_category_name_translation.csv'
    ]
    data_frames = load_data(files_list)
    clean_data(data_frames)
    order_payments, order_items = aggregate_data(data_frames)
    big_table = merge_data(data_frames, order_items, order_payments)

    print(big_table.dtypes)

    big_table.to_parquet('../test_parquet/big_table_b1', partition_cols=['english_category_name'])

if __name__ == '__main__':
    main()