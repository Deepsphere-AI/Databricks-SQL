# Databricks notebook source
import http.client

conn = http.client.HTTPSConnection("api.litmos.com")

headers = {
      'apikey': "0fa0c8af-0845-4d66-ba17-6c0805b8addd",
    }

conn.request("GET", "/v1.svc/users?source=deepsphereai", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))

# COMMAND ----------



# COMMAND ----------

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

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from uk_public_api;

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

inputPath = "/databricks-datasets/structured-streaming/events/"

# Define the schema to speed up processing
jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

streamingInputDF = (
  spark
    .readStream
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .format("json")
    .load(inputPath)
)

streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.action,
      window(streamingInputDF.time, "1 hour"))
    .count()
)

query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

# COMMAND ----------

# MAGIC %sql 
# MAGIC select action, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, action

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

inputPath = "/databricks-datasets/structured-streaming/events/"

# Define the schema to speed up processing
jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

streamingInputDF = (
  spark
    .readStream
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .format("json")
    .load(inputPath)
)

streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.action,
      window(streamingInputDF.time, "1 hour"))
    .count()
)

query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)
