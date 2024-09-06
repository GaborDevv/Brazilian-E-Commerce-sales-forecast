# Brazilian-E-Commerce-sales-forecast

Dataset can be pulled from here, data schema available as well:

[Brazilian E-Commerce Public Dataset by Olist | Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_orders_dataset.csv)

Goal:
We want to forecast for each item, what are the sales going to be next week.

To-do: 
1. Code to load relevant tables for the task (minimum tables needed), and prepare efficient ETL that builds a dataset on which Data Scientist can continue the work (use pandas) 
2. The output should be in parquet, well partitioned by product
3. The format of output is a single table that can be used for modelling (no need to extract features).
4. python script to run code, that you can pass arguments to
5. A couple of simple pytest tests, and run them in github actions at every PR.
6. Configuration files in yml

Think about the following:
- Which features would you extract and how from the tables? How would you use the remaining tables?
- How would you turn it into an application in production?
- How would you design an application if you knew that you would have to build a similar solution for a couple other countries, and the data schema might be different for them, however, you can get the same underlying data? 
