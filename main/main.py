import sys
from functools import reduce
import pandas as pd
import os
import argparse
import logging
import yaml

from utils import categorize_columns, capitalize_columns, columns_to_datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
log = logging.getLogger("main")


def load_config(config_path):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def load_data(path, files_list):
    """Take a list as parameter and iterate through it, to read them into pandas dfs and putting the dataframes into a dictionary"""
    data_frames = {}
    try:
        for file_path in files_list:
            name = (
                file_path.split("/")[-1]
                .replace(".csv", "")
                .replace("olist_", "")
                .replace("_dataset", "")
            )
            var_name = f"data_{name}"
            data_frames[var_name] = pd.read_csv(f"{path}/{file_path}")
            log.info(f"Loaded new DataFrame: {var_name}")
        return data_frames
    except Exception as e:
        log.critical(f"Failed to load files, reason : {e}")
        sys.exit(1)


def fixing_schemas(dataframes):
    """converting string columns that contain date to datetime, and categorize product categories"""
    columns_to_datetime(
        dataframes["data_orders"],
        [
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
    )
    columns_to_datetime(dataframes["data_order_items"], ["shipping_limit_date"])
    columns_to_datetime(
        dataframes["data_order_reviews"],
        ["review_creation_date", "review_answer_timestamp"],
    )
    columns_to_datetime(dataframes["data_order_items"], ["shipping_limit_date"])
    categorize_columns(dataframes["data_products"], "product_category_name")
    categorize_columns(
        dataframes["data_product_category_name_translation"], "product_category_name"
    )
    categorize_columns(
        dataframes["data_product_category_name_translation"],
        "product_category_name_english",
    )


def write_bronze_layer(dict_of_dataframes, path):
    """Save data to the bronze layer in parquet files"""
    os.makedirs(path, exist_ok=True)
    for key, dataframe in dict_of_dataframes.items():
        try:
            dataframe.to_parquet(f"{path}/{key}_bronze.parquet")
            log.info(f"DataFrame {key} written to {path}")
        except Exception as e:
            log.error(f"Failed to write {key} due to {e}")


def clean_data(data_frames):
    """Drop columns that are supposedly not relevant in sales forecast, make city names Title case"""
    # capitalize city names
    capitalize_columns(data_frames["data_customers"], "customer_city")
    capitalize_columns(data_frames["data_sellers"], "seller_city")
    # remove unnecessary columns (we are building table for sales forecasts)
    data_frames["data_order_items"].drop("shipping_limit_date", axis=1, inplace=True)
    data_frames["data_order_payments"].drop(
        ["payment_type", "payment_sequential", "payment_installments"],
        axis=1,
        inplace=True,
    )
    data_frames["data_order_reviews"].drop(
        ["review_comment_title", "review_comment_message", "review_answer_timestamp"],
        axis=1,
        inplace=True,
    )
    data_frames["data_orders"].drop(
        [
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ],
        axis=1,
        inplace=True,
    )


def write_silver_layer(dict_of_dataframes, path):
    """Save cleaned data to silver layer"""
    os.makedirs(path, exist_ok=True)
    for key, dataframe in dict_of_dataframes.items():
        try:
            dataframe.to_parquet(f"{path}/{key}_silver.parquet")
            log.info(f"DataFrame {key} written to {path}")
        except Exception as e:
            log.error(f"Failed to write {key} due to {e}")


def aggregate_data(data_frames):
    """Aggregate the data to make a couple of summarizations, take the mean of duplicated order reviews, check total price,
    total freight cost of an orders, number of items in an order"""
    data_frames["aggregated_reviews"] = (
        data_frames["data_order_reviews"]
        .groupby("order_id")
        .agg({"review_score": "mean"})
    )
    data_frames["aggregated_order_payments"] = (
        data_frames["data_order_payments"]
        .groupby("order_id")
        .agg(total_payment=("payment_value", "sum"))
    )
    data_frames["aggregated_order_items"] = (
        data_frames["data_order_items"]
        .groupby("order_id")
        .agg(
            {
                "price": "sum",
                "freight_value": "sum",
                "order_item_id": "max",
            }
        )
        .rename(
            columns={
                "price": "total_order_value",
                "freight_value": "total_freight_value",
                "order_item_id": "number_of_items_ordered",
            }
        )
    )


def merge_data(data_frames):
    """Create one big table with all the possibly relevant attributes of orders
    merge the necessary tables into one, categorize columns"""
    products = pd.merge(
        data_frames["data_products"],
        data_frames["data_product_category_name_translation"],
        on="product_category_name",
        how="left",
    ).rename(
        columns={
            "product_category_name": "original_category_name",
            "product_category_name_english": "english_category_name",
        }
    )[
        ["product_id", "original_category_name", "english_category_name"]
    ]

    # join multiple tables on common key column
    orders_assist_list = [
        data_frames["data_orders"],
        data_frames["data_order_items"],
        data_frames["data_order_payments"],
        data_frames["aggregated_order_payments"],
        data_frames["aggregated_order_items"],
        data_frames["aggregated_reviews"],
    ]

    orders_df = reduce(
        lambda left, right: pd.merge(left, right, on="order_id", how="inner"),
        orders_assist_list,
    )
    orders_with_sellers_df = pd.merge(
        orders_df, data_frames["data_sellers"], on="seller_id", how="left"
    )

    with_customers_df = pd.merge(
        orders_with_sellers_df,
        data_frames["data_customers"],
        on="customer_id",
        how="left",
    )

    big_table = pd.merge(
        with_customers_df, products, on="product_id", how="inner"
    ).drop(["order_status", "customer_id", "customer_unique_id"], axis=1)
    return big_table


def write_gold_layer(data_frame, path, partition_on):
    """Save the final result to gold layer. Partition by product categories. There is too many items to partition on that"""
    os.makedirs(path, exist_ok=True)
    file_path = f"{path}/big_table_gold.parquet"
    try:
        data_frame.to_parquet(file_path, partition_cols=partition_on)
        log.info(f"DataFrame written to {file_path}")
    except Exception as e:
        log.error(f"Failed to write {data_frame} due to {e}")


def main(args):
    config = load_config(args.config_file)
    files_list = config["files"]
    input_folder = config["paths"]["input_folder"]
    output_folder = config["paths"]["output_folder"]

    bronze_path = f"{output_folder}/{config['paths']['bronze_layer']}"
    data_frames = load_data(input_folder, files_list)
    fixing_schemas(data_frames)
    write_bronze_layer(data_frames, bronze_path)
    clean_data(data_frames)
    silver_path = f"{output_folder}/{config['paths']['silver_layer']}"
    write_silver_layer(data_frames, silver_path)
    aggregate_data(data_frames)
    big_table = merge_data(data_frames)
    gold_path = f"{output_folder}/{config['paths']['gold_layer']}"
    partition_cols = config["partition_columns"]
    write_gold_layer(big_table, gold_path, partition_cols)


def arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_file",
        type=str,
        required=False,
        default="param_config.yaml",
        help="Path to the configuration file",
    )
    arguments = parser.parse_args()
    return arguments


if __name__ == "__main__":
    # Get arguments for source and target folder location
    args = arg_parser()
    main(args)
