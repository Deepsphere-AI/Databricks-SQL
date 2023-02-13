-- Databricks notebook source
-- MAGIC %md #USE Command
-- MAGIC The use command is used when there are multiple databases in the SQL and the user or programmer specifically wants to use a particular database. Thus, in simple terms, the use statement selects a specific database and then performs operations on it using the inbuilt commands of SQL.

-- COMMAND ----------

USE dsai_sales_analysis;

-- COMMAND ----------

-- MAGIC %md #CREATE TABLE USING CSV
-- MAGIC 
-- MAGIC Create table from CSV File loaded in DFS.

-- COMMAND ----------

CREATE TABLE Product_Group_CSV
USING csv
OPTIONS (path "/FileStore/dsai_sales_analysis/Product_Group.csv", header "true");

-- COMMAND ----------

-- MAGIC %md #CREATE Table USING DELTA
-- MAGIC The data management tool that combines the scale of a data lake, the reliability and performance of a data warehouse and the low latency of streaming in a single system for the first time is called Databricks Delta.

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.dsai_sales_analysis.DSAI_product_group(
  Product_Family_ID string NOT NULL,
  Product_Group_ID string NOT NULL,
  Product_Group_Name string NOT NULL
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md #External Table
-- MAGIC 
-- MAGIC An external table references an external storage path using a **LOCATION** clause.
-- MAGIC This storage path should be in an existing external location to which the access has been granted access.
-- MAGIC Alternatively, refer to a storage credential to which the access has been granted.
-- MAGIC Using external tables abstracts away the storage path, external location, and storage credentials for users who have access to the external table.
-- MAGIC 
-- MAGIC ##Location
-- MAGIC The path must be a STRING literal. If there is no specific location, the table is created as managed table and databricks create a default table location. Specifying a location makes the table an external table.

-- COMMAND ----------

-- MAGIC %md ##EXTERNAL TABLE IN AWS S3 STORAGE

-- COMMAND ----------

CREATE OR REPLACE TABLE hive_metastore.dsai_sales_analysis.DSAI_product_group(
  Product_Family_ID string NOT NULL,
  Product_Group_ID string NOT NULL,
  Product_Group_Name string NOT NULL
) USING DELTA
Location 's3://airline-data-bucket/external-storage/dsai_product_group';


-- COMMAND ----------

-- MAGIC %md ##EXTERNAL STORAGE IN GCP

-- COMMAND ----------

CREATE OR REPLACE TABLE dsai_sales_analysis.DSAI_product_family(
  Product_Family_ID string Not NULL ,
  Product_Family_Name string NOT NULL
)USING DELTA
Location 'gs://dsai-sales-analysis-gcp-bucket/external-storage/product_family.csv';

-- COMMAND ----------

-- MAGIC %md #COPY INTO
-- MAGIC It loads data from a file location into a Delta table.
-- MAGIC 
-- MAGIC ##FILE
-- MAGIC Databricks File System or DBFS is a distributed file system mounted into a Databricks workspace and available on Databricks clusters. The DBFS - is an abstraction on top of scalable object storage that provides an optimised FUSE (Filesystem in Userspace) interface that maps to native cloud storage API calls.
-- MAGIC 
-- MAGIC ##FILEFORMAT
-- MAGIC Apache Parquet is a columnar file format providing optimisations to speed up queries which are far more efficient file formats than CSV or JSON.

-- COMMAND ----------

COPY INTO dsai_sales_analysis.dsai_product_group_ext
FROM '/FileStore/dsai_sales_analysis/Product_Group.csv'
FILEFORMAT = CSV
FORMAT_OPTIONS (
  'HEADER' = 'TRUE'
)
COPY_OPTIONS ('FORCE'='TRUE');

-- COMMAND ----------

-- MAGIC %md #Data through API
-- MAGIC 
-- MAGIC   **import http**
-- MAGIC         This is python in-built library, to make http connection and receive response.
-- MAGIC 
-- MAGIC   **http.client.HTTPSConnection**
-- MAGIC        This function is used to establish connection with the URL.
-- MAGIC        
-- MAGIC   **connection.request**
-- MAGIC         This function is used to send request parameters to the URL
-- MAGIC         
-- MAGIC   **conn.getresponse**
-- MAGIC         This function is used to read the response and status from the request URL.

-- COMMAND ----------

import http
import json
def call_api(table_name):
  status = ''
  execution_log = ''
  try:
    conn = http.client.HTTPSConnection("api.postcodes.io")
    payload = ''
    headers = {
              'Cookie': '__cfduid=d2e270bea97599e2fbde210bf483fcd491615195032'
              }
    for val in range(2):
      conn.request("GET", "/random/postcodes", payload, headers)
      execution_log += 'connection is done'
      res = conn.getresponse()
      data = res.read().decode("utf-8")
      jsondata = json.loads(json.dumps(data))
      print (jsondata)
      execution_log += 'JSON payload is done'
      df = spark.read.json(sc.parallelize([jsondata]))
      if val == 0:
        df_temp = df.selectExpr("string(status) as status","result['country'] as country", "result['european_electoral_region'] as european_electoral_region", "string(result['latitude']) as latitude", "string(result['longitude']) as longitude", "result['parliamentary_constituency'] as parliamentary_constituency", "result['region'] as region","'' as vld_status","'' as vld_status_reason")
        df_union = df_temp
      else:
        df_union = df_union.union(df_temp)
      df_union.write.format("parquet").mode("append").saveAsTable(f"{table_name}")

    #your work
    status = 'success'
    execution_log = f"call_publicapi - success - created successfully"
  except Exception as execution_error:
    status = 'failed'
    execution_log = f"call_publicapi - failed - with error {str(execution_error)}"
  return status, execution_log


call_api('uk_public_api')

-- COMMAND ----------

-- MAGIC %md #DESCRCRIBE 
-- MAGIC Returns the basic metadata of the table. It includes column name, column type, column comment and storage location.

-- COMMAND ----------

DESCRIBE EXTENDED hive_metastore.dsai_sales_analysis.dsai_product_group_ext;

-- COMMAND ----------

-- MAGIC %md #Drop Extended Table
-- MAGIC 
-- MAGIC Suppose the table is not an EXTERNAL table. In that case, it deletes the table and removes the associated directory with the table from the file system. An oddity is thrown if the table does not exist. In the case of an external table, only the associated metadata information is removed from the metastore schema.

-- COMMAND ----------

DROP TABLE dsai_proudct_group_ext;

-- COMMAND ----------

-- MAGIC %md #External Table Data
-- MAGIC 
-- MAGIC Dropping EXTERNAL TABLES will not delete data. Instead, use TRUNCATE TABLE to get rid of data

-- COMMAND ----------

SELECT * FROM delta.`s3://airline-data-bucket/external-storage/dsai_product_group`;

-- COMMAND ----------

SELECT count(*) FROM delta.`s3://airline-data-bucket/external-storage/dsai_product_group`;

-- COMMAND ----------

-- MAGIC %md #GRANT AND PRIVILEGE

-- COMMAND ----------

-- MAGIC %md ##GRANT 
-- MAGIC Grants a privilege on a securable object to a principal. Modifying access to the samples Catalogue is not supported. This Catalogue is available to all workspaces but is read-only. Use GRANT ON SHARE to grant recipients access to shares
-- MAGIC ## PRIVILEGE
-- MAGIC The table access control is enabled on the workspace. All clusters and SQL objects in Azure Databricks are hierarchical. Privileges are inherited downward, which means granting or denying a privilege on the Catalogue automatically grants or denies the privilege to all schemas in the Catalogue.

-- COMMAND ----------

GRANT SELECT ON TABLE hive_metastore.dsai_sales_analysis.dsai_fact TO user1;

-- COMMAND ----------

-- MAGIC %md ##SHOW GRANT
-- MAGIC 
-- MAGIC Displays all inherited, denied and granted privileges that affect the securable object. 
-- MAGIC To run this command, use either:
-- MAGIC A Workspace administrator or the owner of the object.
-- MAGIC User specified in principal.
-- MAGIC 
-- MAGIC Use SHOW GRANTS TO RECIPIENT to list the shares a recipient has access to.

-- COMMAND ----------

SHOW GRANTS `user1` ON SCHEMA hive_metastore.dsai_sales_analysis;

-- COMMAND ----------

-- MAGIC %md ##REVOKE
-- MAGIC Revokes explicitly granted or denied privilege on a securable object from a principal. Modifying access to the samples Catalogue is not supported, which is available to all workspaces but is read-only.

-- COMMAND ----------

REVOKE SELECT ON TABLE hive_metastore.dsai_sales_analysis.dsai_fact FROM user1;

-- COMMAND ----------

-- MAGIC %md #DISTINCT
-- MAGIC 
-- MAGIC It tests if the arguments have different values where NULLs are considered comparable.

-- COMMAND ----------

SELECT DISTINCT(Product_Family_ID) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md #WHERE
-- MAGIC It limits the results of the FROM clause of a query or a subquery based on the specified condition.

-- COMMAND ----------

SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_fact WHERE Product_Family_ID = 'PF112';

-- COMMAND ----------

-- MAGIC %md #ORDER BY
-- MAGIC The user-specified order where returns the result rows in a sorted manner. Contrary to the SORT BY clause, this clause guarantees total order in the output.

-- COMMAND ----------

SELECT Product_Family_ID,count(*) FROM hive_metastore.dsai_sales_analysis.dsai_fact GROUP BY Product_Family_ID
ORDER BY Product_Family_ID;

-- COMMAND ----------

SELECT Product_Family_ID, count(*)
FROM hive_metastore.dsai_sales_analysis.dsai_fact
GROUP BY Product_Family_ID
ORDER BY 2 ASC;

-- COMMAND ----------

-- MAGIC %md #STRING FUNCTIONS

-- COMMAND ----------

-- MAGIC %md ##STRING FUNCTION
-- MAGIC Casts the value expr to STRING.
-- MAGIC string(expr)

-- COMMAND ----------

select string(Sales_date) from hive_metastore.dsai_sales_analysis.dsai_sales_date;

-- COMMAND ----------

-- MAGIC %md ##CONCAT_WS FUNCTION
-- MAGIC 
-- MAGIC This function is used to add two or more words or strings with specified separator.

-- COMMAND ----------

SELECT CONCAT_WS('-',Product_Family_ID, Product_ID,'') FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md ##CONCAT using ||
-- MAGIC This function is used to add two or more words or strings.

-- COMMAND ----------

SELECT Product_Family_ID || Product_ID FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md ##FORMAT_STRING
-- MAGIC This function is used to display a string in the given format.

-- COMMAND ----------

SELECT format_string('Product ID %s %s = %s', Product_ID,Unit_Price_Name ,Unit_Price) FROM hive_metastore.dsai_sales_analysis.dsai_unit_price;


-- COMMAND ----------

-- MAGIC %md ##SUBSTRING
-- MAGIC Returns the substring of string that starts at pos and is of length len.
-- MAGIC 
-- MAGIC substring(string, pos [, len])
-- MAGIC 
-- MAGIC substring(string FROM pos [FOR len] ] )

-- COMMAND ----------

SELECT Product_ID, Unit_Price, SUBSTRING(Unit_Price_Name, 1 ,3) FROM hive_metastore.dsai_sales_analysis.dsai_unit_price;

-- COMMAND ----------

-- MAGIC %md ##STARTSWITH
-- MAGIC This function used to check the wheather the column value starting with particular string .
-- MAGIC 
-- MAGIC Returns true if expr begins with startExpr.
-- MAGIC 
-- MAGIC **STARTSWIDTH(expr, startExpr)**

-- COMMAND ----------

SELECT Sales_Country_Name FROM hive_metastore.dsai_sales_analysis.dsai_sales_country
WHERE STARTSWITH(Sales_Country_Name, 'A') = true;


-- COMMAND ----------

-- MAGIC %md ##ENDSWITH
-- MAGIC 
-- MAGIC Returns true if expr ends with endExpr.
-- MAGIC **ENDSWITH(expr, endExpr)**

-- COMMAND ----------

SELECT Sales_Country_Name FROM hive_metastore.dsai_sales_analysis.dsai_sales_country
WHERE ENDSWITH(Sales_Country_Name, 'a') = true;

-- COMMAND ----------

-- MAGIC %md ##CONTAINS
-- MAGIC This function is used to check the search string available in the column.
-- MAGIC 
-- MAGIC Returns true if expr contains subExpr.
-- MAGIC 
-- MAGIC **contains(expr, subExpr)**

-- COMMAND ----------

SELECT Sales_Country_Name FROM hive_metastore.dsai_sales_analysis.dsai_sales_country
WHERE CONTAINS(Sales_Country_Name, 'Ba') = true;

-- COMMAND ----------

-- MAGIC %md ##INITCAP
-- MAGIC Returns the string with initial character in upper case and all other characters in lowere case.

-- COMMAND ----------

select initcap(Customer_Full_Name) from hive_metastore.dsai_sales_analysis.dsai_customer;

-- COMMAND ----------

-- MAGIC %md ##LENGTH Function
-- MAGIC Returns the character length of string data or number of bytes of binary data.
-- MAGIC The length of string data includes the trailing spaces. The length of binary data includes trailing binary zeros.

-- COMMAND ----------

SELECT Customer_Full_Name,LENGTH(Customer_Full_Name)  from hive_metastore.dsai_sales_analysis.dsai_customer;

-- COMMAND ----------

-- MAGIC %md ##TRIM
-- MAGIC Removes the leading and trailing space characters from stingr.
-- MAGIC Removes the leading and trailing trimString characters from string.

-- COMMAND ----------

SELECT TRIM('    DATABRICKS   '), TRIM ('BRICKS' FROM 'DATABRICKS');

-- COMMAND ----------

-- MAGIC %md ##LPAD
-- MAGIC Returns string, left-padded the given string with pad to a length specified. 
-- MAGIC 
-- MAGIC If pad string not given it return the string left padded with space to the length specified.
-- MAGIC 
-- MAGIC If given string is longer than len, the return value is shortened to len characters.
-- MAGIC 
-- MAGIC lpad(expr, len [, pad] )

-- COMMAND ----------

SELECT LPAD('hi', 5, 'ab');

-- COMMAND ----------

-- MAGIC %md ##RPAD
-- MAGIC Returns string, right-padded the given string with pad to a length specified. 
-- MAGIC 
-- MAGIC If pad string not given it return the string right padded with space to the length specified.
-- MAGIC 
-- MAGIC If given string is longer than len, the return value is shortened to len characters.
-- MAGIC 
-- MAGIC rpad(expr, len [, pad] )

-- COMMAND ----------

SELECT RPAD('hi', 5, 'ab');

-- COMMAND ----------

-- MAGIC %md #Date FUNCTIONS

-- COMMAND ----------

-- MAGIC %md ##DATE
-- MAGIC The expression to cast DATE and the value expr to DATE.

-- COMMAND ----------

SELECT DATE(CREATED_DT) FROM dsai_sales_analysis.dsai_sales_date

-- COMMAND ----------

-- MAGIC %md ##CURRENT_DATE
-- MAGIC This function returns CURRENT DATE

-- COMMAND ----------

SELECT current_date();

-- COMMAND ----------

-- MAGIC %md ##NOW 
-- MAGIC Return current date and time

-- COMMAND ----------

SELECT now();

-- COMMAND ----------

-- MAGIC %md ##CURRENT_TIMESTAMP
-- MAGIC This function return current date and time.

-- COMMAND ----------

SELECT current_timestamp();

-- COMMAND ----------

-- MAGIC %md ##CURRENT_TIMEZONE
-- MAGIC 
-- MAGIC Returns the current session local timezone.

-- COMMAND ----------

SELECT current_timezone();

-- COMMAND ----------

-- MAGIC %md ##DATE_ADD Function
-- MAGIC This function used to add num days to the Date.

-- COMMAND ----------

SELECT DATE_ADD(Sales_Date,1) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md ##DATE_SUB FUNCTION
-- MAGIC This function is used to subtract num days from the Date.

-- COMMAND ----------

SELECT Sales_Date,date_sub(Sales_Date,3) FROM hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md ##ADD_MONTHS
-- MAGIC This function is used to add num months to the Date.

-- COMMAND ----------

SELECT date(Sales_Date),add_months(date(Sales_Date),2) from hive_metastore.dsai_sales_analysis.dsai_fact limit 1;

-- COMMAND ----------

-- MAGIC %md ##DATEDIFF
-- MAGIC This function return difference between two dates.

-- COMMAND ----------

SELECT DATE(Sales_Date),DATEDIFF(now(),date(Sales_Date)) from hive_metastore.dsai_sales_analysis.dsai_fact limit 1;

-- COMMAND ----------

SELECT date(Sales_Date),datediff('2022-08-02',date(Sales_Date)) from hive_metastore.dsai_sales_analysis.dsai_fact limit 1;

-- COMMAND ----------

-- MAGIC %md ##MONTHS_BETWEEN
-- MAGIC This function returns number of months between the two dates.

-- COMMAND ----------

SELECT date(Sales_Date),now(),months_between(now(),date(Sales_Date)) from hive_metastore.dsai_sales_analysis.dsai_fact limit 1;

-- COMMAND ----------

-- MAGIC %md #INSERT INTO
-- MAGIC In INSERT INTO statement all rows inserted are additive to the existing rows.

-- COMMAND ----------

INSERT INTO hive_metastore.dsai_sales_analysis.dsai_product_family values ('P115','Garments');

-- COMMAND ----------

-- MAGIC %md #SHOW COLUMNS
-- MAGIC SHOW COLUMNS only shows and views information about the columns in a given table with some privilege.

-- COMMAND ----------

SHOW COLUMNS From hive_metastore.dsai_sales_analysis.dsai_sales_country

-- COMMAND ----------

-- MAGIC %md #INSERT OVERWRITE
-- MAGIC If the table is Without a partition_spec the table is truncated before inserting the first row.
-- MAGIC Otherwise, all partitions matching the partition_spec are truncated before inserting the first row.

-- COMMAND ----------

INSERT OVERWRITE LOCAL DIRECTORY '/FileStore/dsai_sales_analysis/'
    STORED AS orc
    SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_product_family;

-- COMMAND ----------

-- MAGIC %md #Case When
-- MAGIC 
-- MAGIC The CASE expression is used to check multiple condition.
-- MAGIC 
-- MAGIC It goes through conditions till the condition becomes true. Once the condition is true, it will stop reading next condition and return the result. 
-- MAGIC 
-- MAGIC If no conditions are true, it returns the value in the ELSE clause.
-- MAGIC 
-- MAGIC If there is no ELSE part and no conditions are true, it returns NULL.

-- COMMAND ----------

SELECT Customer_ID,Quantity_Sold,CASE WHEN int(Quantity_Sold)  >= 3 THEN 'High Potential' WHEN Quantity_Sold  < 3 and Quantity_Sold >=2 THEN 'Low Potential' ELSE 'Normal' END
from hive_metastore.dsai_sales_analysis.dsai_fact;

-- COMMAND ----------

-- MAGIC %md #JOINS
-- MAGIC Based on join criteria, it combines rows from two relations.

-- COMMAND ----------

-- MAGIC %md ##INNER JOINS
-- MAGIC 
-- MAGIC Returns rows that have matching values in both relations. The default join.

-- COMMAND ----------

select pf.Product_Family_ID,pf.Product_Family_Name,count(Quantity_Sold) from hive_metastore.dsai_sales_analysis.dsai_product_family pf 
join hive_metastore.dsai_sales_analysis.dsai_fact f on (f.Product_Family_ID = pf.Product_Family_ID)
group by pf.Product_Family_ID,pf.Product_Family_Name;

-- COMMAND ----------

-- MAGIC %md ##Outer Join

-- COMMAND ----------

-- MAGIC %md ###LEFT OUTER JOIN
-- MAGIC The LEFT JOIN also called as LEFT OUTER JOIN returns all records from the left table (table1), and the matching records from the right table (table2).

-- COMMAND ----------

SELECT pf.Product_Family_ID,pf.Product_Family_Name,count(Quantity_Sold) from hive_metastore.dsai_sales_analysis.dsai_product_family pf 
LEFT JOIN hive_metastore.dsai_sales_analysis.dsai_fact f on (f.Product_Family_ID = pf.Product_Family_ID)
GROUP BY pf.Product_Family_ID,pf.Product_Family_Name;

-- COMMAND ----------

-- MAGIC %md ###RIGHT OUTER JOIN
-- MAGIC The RIGHT OUTER JOIN also simply say RIGHT JOIN returns all records from the right table (table2), and the matching records from the left table (table1). 

-- COMMAND ----------

SELECT pf.Product_Family_ID,pf.Product_Family_Name,count(Quantity_Sold) from hive_metastore.dsai_sales_analysis.dsai_product_family pf 
RIGHT JOIN hive_metastore.dsai_sales_analysis.dsai_fact f on (f.Product_Family_ID = pf.Product_Family_ID)
GROUP BY pf.Product_Family_ID,pf.Product_Family_Name;

-- COMMAND ----------

-- MAGIC %md ###FULL OUTER JOIN
-- MAGIC Returns all the rows from the joined tables, whether they are matched or not.
-- MAGIC Combines the functionality of both LEFT and RIGHT JOIN

-- COMMAND ----------

SELECT pf.Product_Family_ID,pf.Product_Family_Name,count(Quantity_Sold) from hive_metastore.dsai_sales_analysis.dsai_product_family pf 
FULL JOIN hive_metastore.dsai_sales_analysis.dsai_fact f on (f.Product_Family_ID = pf.Product_Family_ID)
GROUP BY pf.Product_Family_ID,pf.Product_Family_Name;

-- COMMAND ----------

-- MAGIC %md ##CROSS JOIN
-- MAGIC Cross join returns the Cartesian product of rows from the tables in the join. It combines each row from the first table with each row from the second.

-- COMMAND ----------

select * from hive_metastore.dsai_sales_analysis.dsai_sales_date c
CROSS JOIN hive_metastore.dsai_sales_analysis.dsai_sales_region r

-- COMMAND ----------

-- MAGIC %md #SubQuery
-- MAGIC The subquery is also referred to as sub- SELECT s or nested SELECT s. The full SELECT syntax is valid in subqueries and appears inside another query statement.

-- COMMAND ----------

SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_fact
WHERE Customer_ID IN (
    SELECT Customer_ID FROM hive_metastore.dsai_sales_analysis.dsai_fact
    WHERE Revenue > 20
)

-- COMMAND ----------

-- MAGIC %md #VERSION AS OF
-- MAGIC This function is used to retrive data from the delta table using version number.

-- COMMAND ----------

-- Describe history hive_metastore.dsai_sales_analysis.dsai_fact;
SELECT * FROM delta.`dbfs:/user/hive/warehouse/dsai_sales_analysis.db/dsai_fact` VERSION AS OF 8;

-- COMMAND ----------

-- MAGIC %md #TIMESTAMP AS OF
-- MAGIC This command is used to retrive the data of delta table from history using timestamp.

-- COMMAND ----------

--Describe history hive_metastore.dsai_sales_analysis.dsai_fact;
SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_fact TIMESTAMP AS OF '2022-08-26T12:01:03.000'

-- COMMAND ----------

-- MAGIC %md #UNION

-- COMMAND ----------

-- MAGIC %md ##UNION / UNION DISTINCT
-- MAGIC 
-- MAGIC This command is used to combine the resultset of two or more select statements.
-- MAGIC 
-- MAGIC Every SELECT statement must have the same number of columns ,similar data types and also be in the same order.
-- MAGIC 
-- MAGIC It skip duplicate rows.

-- COMMAND ----------

SELECT * from hive_metastore.dsai_sales_analysis.dsai_product_family 
UNION SELECT * from hive_metastore.dsai_sales_analysis.dsai_product_family_ext;

-- COMMAND ----------

-- MAGIC %md ##UNION ALL
-- MAGIC 
-- MAGIC This command is used to combine the resultset of two or more select statements.
-- MAGIC 
-- MAGIC Every SELECT statement must have the same number of columns ,similar data types and also be in the same order.
-- MAGIC 
-- MAGIC The UNION command selects only distinct values by default. To allow duplicate values, use UNION ALL.

-- COMMAND ----------

SELECT * FROM hive_metastore.dsai_sales_analysis.dsai_product_family 
UNION ALL select * from hive_metastore.dsai_sales_analysis.dsai_product_family_ext;

-- COMMAND ----------

-- MAGIC %md #SELECT AS
-- MAGIC 
-- MAGIC This SELECT AS composes a result set from one or more tables. It can be part of a query that includes common table expressions (CTE), set operations and other clauses.

-- COMMAND ----------

SELECT Product_Family_ID,Product_Family_Name, concat_ws('-',Product_Family_ID,Product_Family_Name) as Product_FName FROM hive_metastore.dsai_sales_analysis.dsai_product_family as f 

-- COMMAND ----------

-- MAGIC %md #VIEW
-- MAGIC Based on the SQL query result-set, it constructs a virtual table with no physical data where ALTER VIEW and DROP VIEW only change metadata.

-- COMMAND ----------

CREATE OR REPLACE VIEW hive_metastore.dsai_sales_analysis.consolidated_sales as (
SELECT YEAR(Sales_Date) as syear,MONTH(Sales_Date) as smonth,Customer_ID,sum(Revenue) rev FROM hive_metastore.dsai_sales_analysis.dsai_fact 
GROUP BY syear,smonth,Customer_ID);

SELECT * FROM hive_metastore.dsai_sales_analysis.consolidated_sales where rev>=300 ;

-- COMMAND ----------

-- MAGIC %md #MERGE
-- MAGIC IF THERE IS A MATCH
-- MAGIC The MATCHED clauses are executed when a source row matches a target table row based on the merge_condition and the optional match_condition.
-- MAGIC 
-- MAGIC IF THERE ARE NO MATCHES, The MERGE operation can fail if multiple rows of the source dataset match, where an attempt to update the same rows of the target Delta table.

-- COMMAND ----------

MERGE INTO hive_metastore.dsai_sales_analysis.dsai_product_family T
USING hive_metastore.dsai_sales_analysis.dsai_product_family_source S
ON (S.Product_Family_ID = T.Product_Family_ID)
WHEN MATCHED 
     THEN UPDATE 
     SET T.Product_Family_Name = S.Product_Family_Name            
WHEN NOT MATCHED 
     THEN INSERT (Product_Family_ID, Product_Family_Name, created_user, Created_DT, Updated_User, Updated_DT)
     VALUES (S.Product_Family_ID, S.Product_Family_Name, 'DSAI_user',now(),'DSAI_User',now())


-- COMMAND ----------

-- MAGIC %md #AGGREGATE FUNCTIONS

-- COMMAND ----------

-- MAGIC %md ##GROUP BY
-- MAGIC 
-- MAGIC It is used to group the rows based on a set of specified grouping expressions and calculate aggregations on the group of rows based on one or multiple specified aggregate functions. Also, the Databricks Runtime supports advanced aggregations to do multiple aggregations for the same input record set via GROUPING SETS, CUBE, and ROLLUP clauses. 

-- COMMAND ----------

-- MAGIC %md ##COUNT
-- MAGIC The COUNT() function returns the number of rows that matches a specified criterion

-- COMMAND ----------

SELECT Product_Family_ID, count(*)
FROM hive_metastore.dsai_sales_analysis.dsai_fact
GROUP BY Product_Family_ID;

-- COMMAND ----------

-- MAGIC %md ##SUM
-- MAGIC The SUM() function returns the sum value of a numeric column. 

-- COMMAND ----------

SELECT product_family_id,SUM(Revenue) FROM dsai_sales_analysis.dsai_fact group by 1;

-- COMMAND ----------

-- MAGIC %md ##MAX
-- MAGIC The MAX() function Returns the largest value of the selected column

-- COMMAND ----------

SELECT sales_state_id,MAX(Revenue) FROM dsai_sales_analysis.dsai_fact group by sales_state_id;

-- COMMAND ----------

-- MAGIC %md ##MIN
-- MAGIC The MIN() function returns the smallest value of the selected column.

-- COMMAND ----------

SELECT sales_state_id,MIN(Revenue) FROM dsai_sales_analysis.dsai_fact group by sales_state_id;

-- COMMAND ----------

-- MAGIC %md ##AVG
-- MAGIC The AVG() function returns the average value of a numeric column. 

-- COMMAND ----------

SELECT sales_state_id,AVG(Revenue) FROM dsai_sales_analysis.dsai_fact group by sales_state_id;

-- COMMAND ----------

-- MAGIC %md ##Grouping Sets
-- MAGIC 
-- MAGIC Aggregations using multiple sets of grouping columns in a single statement.
-- MAGIC 
-- MAGIC Following performs aggregations based on four sets of grouping columns.
-- MAGIC 
-- MAGIC (1). product_family_id, sales_state_id
-- MAGIC 
-- MAGIC (2). product_family_id
-- MAGIC 
-- MAGIC (3). sales_city_id
-- MAGIC 
-- MAGIC (4). Empty grouping set. Returns quantities for all product_family_id and car sales_state_id.

-- COMMAND ----------

SELECT product_family_id, sales_state_id, sum(Revenue) AS sum
    FROM dsai_fact
    GROUP BY GROUPING SETS ((product_family_id, sales_state_id), (product_family_id), (sales_state_id), ())
    ORDER BY product_family_id,sales_state_id;


-- COMMAND ----------

-- MAGIC %md ##GROUP BY ... WITH ROLLUP
-- MAGIC 
-- MAGIC GROUP BY ROLLUP clause is Equivalent to GROUP BY GROUPING SETS 
-- MAGIC 
-- MAGIC GROUP BY product_family_id, sales_state_id WITH ROLLUP 
-- MAGIC 
-- MAGIC is equivalent to
-- MAGIC 
-- MAGIC GRPOUP BY GROUPING SETS ((product_family_id, sales_state_id), (product_family_id), ())

-- COMMAND ----------

SELECT product_family_id, sales_state_id, sum(Revenue) AS sum
    FROM dsai_fact
    GROUP BY product_family_id, sales_state_id WITH ROLLUP 
    ORDER BY product_family_id,sales_state_id;

-- COMMAND ----------

-- MAGIC %md ##GROUP BY ... WITH CUBE
-- MAGIC 
-- MAGIC GROUP BY CUBE clause is Equivalent to GROUP BY GROUPING SETS
-- MAGIC 
-- MAGIC GROUP BY product_family_id, sales_state_id WITH CUVE
-- MAGIC 
-- MAGIC is equivalent to
-- MAGIC 
-- MAGIC GRPOUP BY GROUPING SETS ((product_family_id, sales_state_id), (product_family_id), (sales_state_id) ())

-- COMMAND ----------

SELECT product_family_id, sales_state_id, sum(Revenue) AS sum
    FROM dsai_fact
    GROUP BY product_family_id, sales_state_id WITH CUBE
    ORDER BY product_family_id,sales_state_id;

-- COMMAND ----------

-- MAGIC %md #UPDATE
-- MAGIC When no predicate is provided, it updates the column values for all the rows. If any predicate is provided, it updates the column values for rows that match a predicate.

-- COMMAND ----------

UPDATE dsai_fact SET currency = 'USD';

-- COMMAND ----------

UPDATE dsai_fact SET Quantity_Sold = 3,Revenue=200 WHERE sales_id=1;

-- COMMAND ----------

-- MAGIC %md #Calculated Column
-- MAGIC 
-- MAGIC Using functions or operators, this formula computes a result based on literal references to columns, fields, or variables.

-- COMMAND ----------

SELECT Sales_Date,Customer_ID,Product_Family_ID,Product_ID,Revenue,Cost_Of_Goods_Sold,Labor_Cost,
Operating_Cost,(Revenue-(Cost_Of_Goods_Sold+Labor_Cost+Operating_Cost)) AS Profit 
FROM hive_metastore.dsai_sales_analysis.dsai_fact 
