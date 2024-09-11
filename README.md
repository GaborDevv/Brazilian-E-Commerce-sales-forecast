# Brazilian-E-Commerce-sales-forecast

  This project uses a Brazilian ecommerce public dataset of orders made at Olist Store. The data has information of 100k orders from 2016 to 2018 made at multiple marketplaces in Brazil.     Its features allows viewing an order from multiple dimensions: from order status, price, payment and freight performance to customer location, product attributes and finally reviews        written by customers. This is real commercial data, it has been anonymised, and references to the companies and partners in the review text have been replaced with the names of Game of     Thrones great houses.


## **Link to the dataset**
  [Brazilian E-Commerce Public Dataset by Olist | Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce/data?select=olist_orders_dataset.csv)


### **Goal:**

  We want a forecast for each item, what are the sales going to be next week.


## **Project Status** 
  
  ✅ Code to load relevant tables for the task (minimum tables needed), and prepare efficient ETL that builds a dataset on which Data Scientist can continue the work (use pandas). --
  I used pandas during the whole ETL because the data size did not require spark.
  
  ✅ The output should be in parquet, well partitioned by product. -- I partitioned by product category in english, because there were too many products to partition them effectively.
  
  ✅ The format of output is a single table that can be used for modelling.
  
  ✅ Python script to run code, that you can pass arguments to.
  
  ✅ A couple of simple pytest tests, and run them in github actions at every PR. -- I've added manual trigger as well
  
  ✅ Configuration files in yml.


## **Think about the following:**

### **- Which features would you extract and how from the tables? How would you use the remaining tables?**  

  For feature extraction there is a lot of options. For example if we wanted to create a correlation matrix to see what influences prices and how, we could extract ratings, prices (item      price and/or freight price), check for seasonality -> which months have more volume, is there a holiday coming up(e.g. christmas). We could check  sales geographically as well, to check    how prices changes correlate with sales volume in states. 
  The rest of the tables, columns could be used for other predictions, improvements. We could build models for logistics with shipping data or we could learn about marketing insights,        which region should the company concentrate on with the review data.
  
### **- How would you turn it into an application in production?**

  I could possibly create some UI, with dashboards. For example we could use the Dash framework which is open-source and makes it easier to build data visualization interfaces. 
  I would create Machine Learning models, pipelines that could populate these dashboards, create forecasts, heatmaps for product popularity in cities/states (eg. to optimize storage) 

### **- How would you design an application if you knew that you would have to build a similar solution for a couple other countries, and the data schema might be different for them, however, you can get the same underlying data?**

  In my gold layer I would use unified schemas for the underlying data, and during my ETL pipeline I would transform the new tables to be able to work with different countries with the       same logic. 

  
## **Run the script locally**

### Pre-requirements: 
  python installed to the local environment. It is recommended to create a virtual environment before running the setup.py script.
### Steps
  1. Copy the repository
  2. *[Create a kaggle account](https://www.kaggle.com/account/login?phase=startRegisterTab&returnUrl=%2F), if you don't have one already 
  3. *Create and download your API key
  4. *In your home folder create a .kaggle folder
  5. *Add the downloaded .json file to this .kaggle folder
  6. **Execute:
    ```sh
    python setup.py --dataset_download Yes --data_location <path/to/raw_data>
    ```
  7. ***Execute
   ```sh
   python main/main.py --input_folder <path/to/raw_data> --output_folder <path/to/save_folder>
   ```

  *I included the raw dataset in the repo, so if you dowload that as well, you can skip steps 2-5. In step 6. run the script like this:
  ```sh 
  python setup.py --dataset_download No --data_location <path/to/raw_data>
  ```
    
  ** --dataset_download is a **required** argument, you can enter **yes** or **no**. --data_location is not required, default: raw_data folder. This function installs the dependencies and downloads the dataset if needed
  
  *** Both arguments are **optional**, default: --input_folder raw_data (location of source folder), --output_folder storage (location of target folder)
  
  **ER diagram of the tables ingested into the bronze layer**
  ![image](https://github.com/user-attachments/assets/416296e3-3f93-4739-b116-3dc9cf7bb55a)
  
