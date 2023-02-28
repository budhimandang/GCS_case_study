# Databricks notebook source
import json
import sys
import requests
from pyspark.sql import *
import datetime as dt
currentDate=dt.datetime.now().strftime("%Y-%m-%d %HH-%MM")

def get_web_data(url):
    response=requests.get(url)
    responseJson=json.loads(response.text)
    return response.text,responseJson

bike_id=input("Enter the bike_id to be searched:")

url="https://bikeindex.org/api/v3/bikes/"+bike_id

outResult,outResultJson = get_web_data(url)

raw_file_path="dbfs:/FileStore/restApi/bikeInfo/getBikeInfo.json"

dbutils.fs.rm(raw_file_path)

try:
    dbutils.fs.put(raw_file_path,outResult)
except Exception as e:
    print(str(e))

# COMMAND ----------

df=spark.read.option("multiline",True).json("dbfs:/FileStore/restApi/bikeInfo/getBikeInfo.json")

# COMMAND ----------

display(df)

# COMMAND ----------

if (df.filter(col("bike.additional_registration").isNull()).count() >0 & df.filter(col("bike.stolen_record").isNull()).count() == 0) :
     df2=df2.select('bike.frame_size','bike.manufacturer_name',
'bike.registry_url','bike.api_url','bike.front_gear_type_slug','bike.name','bike.serial',
'bike.url','bike.components','bike.front_tire_narrow','bike.paint_description',
'bike.status','bike.year','bike.date_stolen','bike.front_wheel_size_iso_bsd',
'bike.public_images','bike.stolen','bike.stolen_record.create_open311','bike.description',
'bike.handlebar_type_slug','bike.rear_gear_type_slug','bike.stolen_coordinates',
'bike.stolen_record.police_report_department','bike.external_id',
'bike.id','bike.rear_tire_narrow','bike.stolen_location','bike.stolen_record.police_report_number',
'bike.extra_registration_number','bike.is_stock_img','bike.rear_wheel_size_iso_bsd','bike.test_bike',
'bike.stolen_record.theft_description','bike.frame_colors','bike.large_img',
'bike.registration_created_at','bike.thumb','bike.stolen_record.created_at','bike.frame_material_slug',
'bike.location_found','bike.registration_updated_at','bike.title','bike.stolen_record.latitude','bike.frame_model',
'bike.manufacturer_id','bike.registry_name','bike.type_of_cycle','bike.stolen_record.lock_defeat_description',
'bike.stolen_record.locking_description','bike.stolen_record.longitude')
elif df.filter(col("bike.stolen_record").isNull()).count() == 0 :
     df2=df2.select('bike.additional_registration','bike.frame_size','bike.manufacturer_name',
'bike.registry_url','bike.api_url','bike.front_gear_type_slug','bike.name','bike.serial',
'bike.url','bike.components','bike.front_tire_narrow','bike.paint_description',
'bike.status','bike.year','bike.date_stolen','bike.front_wheel_size_iso_bsd',
'bike.public_images','bike.stolen','bike.stolen_record.create_open311','bike.description',
'bike.handlebar_type_slug','bike.rear_gear_type_slug','bike.stolen_coordinates',
'bike.stolen_record.police_report_department','bike.external_id',
'bike.id','bike.rear_tire_narrow','bike.stolen_location','bike.stolen_record.police_report_number',
'bike.extra_registration_number','bike.is_stock_img','bike.rear_wheel_size_iso_bsd','bike.test_bike',
'bike.stolen_record.theft_description','bike.frame_colors','bike.large_img',
'bike.registration_created_at','bike.thumb','bike.stolen_record.created_at','bike.frame_material_slug',
'bike.location_found','bike.registration_updated_at','bike.title','bike.stolen_record.latitude','bike.frame_model',
'bike.manufacturer_id','bike.registry_name','bike.type_of_cycle','bike.stolen_record.lock_defeat_description',
'bike.stolen_record.locking_description','bike.stolen_record.longitude')
else:
    df2=df.select('bike.additional_registration','bike.frame_size','bike.manufacturer_name',
'bike.registry_url','bike.api_url','bike.front_gear_type_slug','bike.name','bike.serial',
'bike.url','bike.components','bike.front_tire_narrow','bike.paint_description','bike.status',
'bike.year','bike.date_stolen','bike.front_wheel_size_iso_bsd','bike.public_images','bike.stolen',
'bike.description','bike.handlebar_type_slug','bike.rear_gear_type_slug','bike.stolen_coordinates',
'bike.external_id','bike.id','bike.rear_tire_narrow','bike.stolen_location','bike.extra_registration_number',
'bike.is_stock_img','bike.rear_wheel_size_iso_bsd','bike.test_bike','bike.frame_colors','bike.large_img',
'bike.registration_created_at','bike.thumb','bike.frame_material_slug','bike.location_found',
'bike.registration_updated_at','bike.title','bike.stolen_record')

# COMMAND ----------

display(df2)

# COMMAND ----------


