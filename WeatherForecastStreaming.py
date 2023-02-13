# Databricks notebook source
import http
import json
def call_weather_api(table_name):
  status = ''
  execution_log = ''
  try:
    conn = http.client.HTTPSConnection("api.open-meteo.com")
    payload = ''
    headers = {
              'Cookie': '__cfduid=d2e270bea97599e2fbde210bf483fcd491615195032'
              }
    for val in range(2):
      conn.request("GET", "/v1/forecast?latitude=52.52&longitude=13.41&current_weather=true&hourly=temperature_2m", payload, headers)
      execution_log += 'connection is done'
      res = conn.getresponse()
      data = res.read().decode("utf-8")
      jsondata = json.loads(json.dumps(data))
#     print (jsondata)
      execution_log += 'JSON payload is done'
      df = spark.read.json(sc.parallelize([jsondata]))
      if val == 0:
        df_temp = df.selectExpr("string(latitude) as latitude", "string(longitude) as longitude", "string(hourly_units['temperature_2m']) as temp_unit", "string(timezone) as timezone","string(current_weather['time']) as temp_time","string(current_weather['temperature']) as temp_temperature","string(current_weather['weathercode']) as weathercode", "string(current_weather['windspeed']) as windspeed", "string(current_weather['winddirection']) as winddirection")
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


call_weather_api('weather_table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from weather_table;

# COMMAND ----------


