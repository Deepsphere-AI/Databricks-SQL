-- Databricks notebook source
-- MAGIC %md #DSAI_product_family

-- COMMAND ----------

--managed Table
USE dsai_sales_analysis;
CREATE OR REPLACE TABLE DSAI_product_family(
Product_Family_ID string Not NULL ,
Product_Family_Name string NOT NULL,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;
COPY INTO dsai_sales_analysis.dsai_product_family
FROM '/FileStore/dsai_sales_analysis/Product_Family.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')


-- COMMAND ----------

select * from dsai_product_family;

-- COMMAND ----------

-- MAGIC %md #DSAI_product_group

-- COMMAND ----------

--Extended Table Storage in AWS
USE dsai_sales_analysis;
CREATE OR REPLACE TABLE DSAI_product_group(
Product_Family_ID string NOT NULL,
Product_Group_ID string NOT NULL,
Product_Group_Name string NOT NULL,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
) USING DELTA
Location 's3://airline-data-bucket/external-storage/product_group.csv';

COPY INTO dsai_sales_analysis.dsai_product_group
FROM '/FileStore/dsai_sales_analysis/Product_Group.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')


-- COMMAND ----------

-- MAGIC %md #Dsai_unit_of_measure

-- COMMAND ----------

--Extended Table AWS
CREATE OR REPLACE TABLE dsai_sales_analysis.DSAI_unit_of_measure(
Unit_Of_Measure STRING NOT NULL,
Unit_Description STRING NOT NULL,
Created_User STRING NOT NULL,
Created_DT STRING NOT NULL,
Updated_User STRING NOT NULL,
Updated_DT STRING NOT NULL
)USING DELTA 
Location 's3://airline-data-bucket/external-storage/unit_of_measure.csv';

COPY INTO dsai_sales_analysis.dSAI_unit_of_measure
FROM '/FileStore/dsai_sales_analysis/UnitOfMeasurement.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE');

-- COMMAND ----------

-- MAGIC %md #DSAI_product

-- COMMAND ----------

--External Data Table in GCP
USE dsai_sales_analysis;
CREATE OR REPLACE TABLE DSAI_product(
Product_Family_ID string Not Null,
Product_Group_ID string Not Null,
Product_ID string Not Null,
Product_Name string Not Null,
Unit_Of_Measure string Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA
Location 'gs://dsai-sales-analysis-gcp-bucket/external-storage/DSAI_Product.csv';

COPY INTO dsai_sales_analysis.dsai_product
FROM '/FileStore/dsai_sales_analysis/Product.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

select * from DSAI_product;

-- COMMAND ----------

-- MAGIC %md #DSAI_SKU

-- COMMAND ----------

USE dsai_sales_analysis;
Drop table DSAI_SKU;
CREATE OR REPLACE TABLE DSAI_SKU(
Product_ID Varchar(10) NOT NULL,
SKU_ID varchar(10) NOT NULL,
SKU_Description varchar(100) NOT NULL,
Unit_Of_Measure string Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA
Location 'gs://dsai-sales-analysis-gcp-bucket/external-storage/DSAI_SKU.csv';

COPY INTO dsai_sales_analysis.dsai_sku
FROM '/FileStore/dsai_sales_analysis/SKU.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_unit_price

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_unit_price(
Product_ID varchar(10) Not Null,
Sku_ID varchar(10),
Unit_Price_ID varchar(10) Not Null,
Unit_Price varchar(10) Not Null,
Unit_Price_Name varchar(100) Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)Using DELTA;

COPY INTO dsai_sales_analysis.dsai_unit_price
FROM '/FileStore/dsai_sales_analysis/Unit_Price.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_sales_date

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_sales_date(
Sales_Date varchar(30) Not Null,
Sales_Year varchar(10) Not Null,
Sales_Quarter varchar(10) Not Null,
Sales_Month varchar(10) Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_sales_date
FROM '/FileStore/dsai_sales_analysis/Sales_Date.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_sales_currency

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_sales_currency(
Currency_Country string not null,
Currency_Code string not null,
Currency_Name string not null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_sales_currency
FROM '/FileStore/dsai_sales_analysis/Sales_Currency.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')


-- COMMAND ----------

-- MAGIC %md #DSAI_customer

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_customer(
Customer_ID string not null,
Customer_Full_Name string not null,
Customer_Address string not null,
Customer_Phone string not null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_customer
FROM '/FileStore/dsai_sales_analysis/Customer.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_loyalty_program

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_loyalty_program(
Loyalty_Program_ID varchar(50) Not Null,
Loyalty_Program_Name varchar(100) Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_loyalty_program
FROM '/FileStore/dsai_sales_analysis/Loyalty_Program.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')


-- COMMAND ----------

-- MAGIC %md #DSAI_customer_loyalty_program

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_customer_loyalty_program(
Loyalty_Program_ID varchar(50) Not Null,
Customer_ID varchar(10) Not Null,
Loyalty_Program_Name varchar(100) Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_loyalty_program
FROM '/FileStore/dsai_sales_analysis/Customer_Loyalty_Program.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')


-- COMMAND ----------

-- MAGIC %md #DSAI_sales_region

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_sales_region(
Sales_Region_ID varchar(50) Not Null,
Sales_Region_Name varchar(100) Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_sales_region
FROM '/FileStore/dsai_sales_analysis/Sales_Region.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_sales_country

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_sales_country(
Sales_Region_ID varchar(50) Not Null,
Sales_Country_ID varchar(50) Not Null,
Sales_Country_Name varchar(100) Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null)Using DELTA;

COPY INTO dsai_sales_analysis.dsai_sales_country
FROM '/FileStore/dsai_sales_analysis/Sales_Country.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')


-- COMMAND ----------

-- MAGIC %md #DSAI_sales_state

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_sales_state(
Sales_Region_ID varchar(50) NOT NULL,
Sales_Country_ID varchar(50) NOT NULL,
Sales_State_ID varchar(50) NOT NULL,
Sales_State_Name varchar(100) NOT NULL,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_sales_state
FROM '/FileStore/dsai_sales_analysis/Sales_State.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_sales_city

-- COMMAND ----------

CREATE OR REPLACE TABLE DSAI_sales_city(
Sales_Region_ID varchar(50) Not Null,
Sales_Country_ID varchar(50) Not Null,
Sales_State_ID varchar(50) Not Null,
Sales_City_ID varchar(50) Not Null,
Sales_City_Name varchar(100) Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_sales_city
FROM '/FileStore/dsai_sales_analysis/Sales_City.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_sales_location

-- COMMAND ----------

USE dsai_sales_analysis;
CREATE OR REPLACE TABLE DSAI_sales_location(
Region_ID varchar(10) NOT NULL,
Country_ID varchar(10) NOT NULL,
State_ID varchar(10) NOT NULL,
City_ID varchar(10) NOT NULL,
Location_ID varchar(10) NOT NULL,
Location_Name varchar(100) NOT NULL,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_sales_location
FROM '/FileStore/dsai_sales_analysis/Sales_Location.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_data_source

-- COMMAND ----------

USE dsai_sales_analysis;
CREATE OR REPLACE TABLE DSAI_data_source(
Data_Source_ID STRING NOT NULL,
Data_Source_Name string NOT NULL,
Data_Source_Description string NOT NULL,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_data_source 
FROM '/FileStore/dsai_sales_analysis/Data_Source.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_fact

-- COMMAND ----------

USE dsai_sales_analysis;
CREATE OR REPLACE TABLE DSAI_fact(
Sales_ID varchar(10) Not Null,
Sales_Date varchar(10) Not Null,
Product_Family_ID varchar(10) Not Null,
Product_ID varchar(10) Not Null,
SKU_ID varchar(10),
Unit_Price_ID varchar(10) Not Null,
Sales_Currency_Code varchar(10) Not Null,
Sales_Region_ID varchar(10) Not Null,
Sales_Country_ID varchar(10) Not Null,
Sales_State_ID varchar(10) Not Null,
Sales_City_ID varchar(10) Not Null,
Customer_ID varchar(10) Not Null,
Loyalty_Program_ID varchar(10) Not Null,
Quantity_Sold varchar(10) Not Null,
Revenue varchar(10) Not Null,
Cost_Of_Goods_Sold varchar(10) Not Null,
Labor_Cost varchar(10) Not Null,
Material_Cost varchar(10) Not Null,
Operating_Cost varchar(10) Not Null,
Profit_Margin varchar(10) Not Null,
Profit_Margin_Perc varchar(10) Not Null,
Promotion_ID varchar(10),
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
) Using DELTA;

COPY INTO dsai_sales_analysis.dsai_fact
FROM '/FileStore/dsai_sales_analysis/FACT.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE');

-- COMMAND ----------

-- MAGIC %md #DSAI_competitor

-- COMMAND ----------



-- COMMAND ----------

USE dsai_sales_analysis;
CREATE OR REPLACE TABLE DSAI_competitor(
Competitor_ID varchar(10) Not Null,
Competitor_Name varchar(100) Not Null,
Competitor_Office varchar(100) Not Null,
Product_ID varchar(10) Not Null,
Promotion string Not Null,
SKU_ID string Not Null,
Sales_Location string Not Null,
Unit_Price string Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_competitor
FROM '/FileStore/dsai_sales_analysis/Competitor.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------

-- MAGIC %md #DSAI_promotion

-- COMMAND ----------

USE dsai_sales_analysis;
CREATE OR REPLACE TABLE DSAI_promotion(
Product_ID varchar(10) Not Null,
Sku_ID varchar(10),
Promotion_ID varchar(10) Not Null,
Promotion_Name varchar(100) Not Null,
Promotion_Discription varchar(100) Not Null,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_promotion
FROM '/FileStore/dsai_sales_analysis/Promotion.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md #DSAI_data_category

-- COMMAND ----------

USE dsai_sales_analysis;
CREATE OR REPLACE TABLE DSAI_data_category(
Data_Category_ID Varchar(10) NOT NULL,
Data_Category_Name varchar(10) NOT NULL,
Data_Category_Description varchar(100) NOT NULL,
Created_User String Not Null,
Created_DT string Not Null,
Updated_User String Not Null,
Updated_DT string Not Null
)USING DELTA;

COPY INTO dsai_sales_analysis.dsai_data_category
FROM '/FileStore/dsai_sales_analysis/Data_Category.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE','mergeSchema'='TRUE')

-- COMMAND ----------


