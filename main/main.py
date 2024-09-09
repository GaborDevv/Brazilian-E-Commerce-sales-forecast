from pyspark.sql import SparkSession

if __name__ == '__main__':

    spark = SparkSession.builder.appName('Sales Forecast App').getOrCreate()

    raw_payments = spark.read.option('header',True).option('inferSchema', True).csv('raw_data/olist_order_payments_dataset.csv')
    product_attributes = spark.read.option('header', True).csv('raw_data/olist_products_dataset.csv')
    sellers = spark.read.option('header', True).csv('raw_data/olist_sellers_dataset.csv')
    product_category_names = spark.read.option('header', True).csv('raw_data/product_category_name_translation.csv')
    items = spark.read.option('header', True).csv('raw_data/olist_order_items_dataset.csv')

    items.show()
    spark.stop()