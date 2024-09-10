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
  For feature extraction there is a lot of options. For example if we wanted to create a correlation matrix to see what influences prices and how, we could extract ratings, prices (item price and/or freight price), check for seasonality -> which months have more volume, is there a holiday coming up(e.g. christmas). We could check  sales geographically as well, to check how prices change statewise.
  
- How would you turn it into an application in production?
  We could possibly create some UI creation with dashboards. For example we could use the Dash framework which is open-source and makes it easier to build data visualization interfaces. 
  I would create Machine Learning models, pipelines that could populate these dashboards

- How would you design an application if you knew that you would have to build a similar solution for a couple other countries, and the data schema might be different for them, however, you can get the same underlying data?
  In my gold layer I would use unified schemas for the underlying data, and during my ETL pipeline I would transform the new tables to be able to work with different countries with the same logic.
  

