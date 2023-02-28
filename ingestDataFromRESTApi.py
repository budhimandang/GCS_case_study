# Databricks notebook source
#dbutils.fs.ls("dbfs:/FileStore/Raw/wepApi")
#dbutils.fs.mkdirs("dbfs:/FileStore/restApi")
#dbutils.fs.mkdirs("dbfs:/FileStore/restApi/target")

# COMMAND ----------

#importing necessary libraries
import json
import sys
import requests
from pyspark.sql import *
import datetime as dt
currentDate=dt.datetime.now().strftime("%Y-%m-%d %HH-%MM")

#function to get the response from REST API endpoints
def get_web_data(url):
    response=requests.get(url)
    responseJson=json.loads(response.text)
    return response.text,responseJson

#REST API endpoint URL
url="https://bikeindex.org/api/v3/search"

#calling the function to get the data from REST API
outResult,outResultJson = get_web_data(url)

#raw file path -> We are keeping the RAW data - JSON
raw_file_path="dbfs:/FileStore/restApi/bike{}.json".format(currentDate)

#writing the data in stage layer
try:
    dbutils.fs.put(raw_file_path,outResult)
#   dbutils.fs.put("dbfs:/FileStore/restApi/bike{}.json".format(currentDate),outResult)
except Exception as e:
    print(str(e))

# COMMAND ----------

#reading from raw layer location into a dataframe
df = spark.read.option("multiline",True).json(raw_file_path)
display(df)

# COMMAND ----------

#separating the records
from pyspark.sql.functions import explode,col
df2=df.select(explode('bikes').alias('data'))
display(df2)

# COMMAND ----------

#convert the JSON object into dataframe
from pyspark.sql.functions import from_unixtime 
df3=df2.select('data.date_stolen','data.frame_colors','data.large_img','data.registry_name','data.status','data.stolen_location','data.url','data.description','data.frame_model','data.location_found','data.registry_url','data.stolen','data.thumb','data.year','data.external_id','data.id','data.manufacturer_name','data.serial','data.stolen_coordinates','data.title').withColumn('date_time', from_unixtime('date_stolen'))
display(df3)

# COMMAND ----------

#data preprocessing - drop the tuple only if it has all the column values as null
df4=df3.na.drop(how="all").show()
#display(df4)

# COMMAND ----------

#controling the version of the data by adding Modification date and the file name
from pyspark.sql.functions import input_file_name,lit
#import pytz
#ist_time_timezone=pytz.timezone('Asia/Kolkata')
mod_date=dt.datetime.now()
auditDf=df3.withColumn("Modified Date",lit(mod_date)).withColumn("file Name",lit(input_file_name()))
display(auditDf)

# COMMAND ----------

#writing the dataframe to the target
#target location
target_file_path="dbfs:/FileStore/restApi/target".format(currentDate)

try:
    auditDf.write.mode("Append").parquet(target_file_path)
except Exception as e:
    print(str(e))


# COMMAND ----------

latest_data = spark.read.parquet("dbfs:/FileStore/restApi/target")
display(latest_data)

# COMMAND ----------

#dbutils.fs.ls("dbfs:/FileStore/restApi/target")

# COMMAND ----------


