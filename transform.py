# Databricks notebook source
# Extract and load function
import requests
import os
import zipfile
import chardet
import io
import pandas as pd
from pyspark.sql import SparkSession


# COMMAND ----------

# Connect to PySpark and transform the dataset
spark = SparkSession.builder.appName("Transform NYC Shooting Data").getOrCreate()

# Read the data from the Delta table
nyc_shooting_df = spark.read.format("delta").table("ids706_data_engineering.jcw131_nyc_shooting.jcw131_nyc_shooting")

# Perform some basic transformations
transformed_df = nyc_shooting_df.select("Incident_Key", "Boro", "PERP_SEX", "Perp_Race", "VIC_SEX", "VIC_RACE", "OCCUR_DATE") \
                                .filter(nyc_shooting_df["Boro"] == "QUEENS")

# Save the transformed data to a new Delta table
transformed_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("ids706_data_engineering.jcw131_nyc_shooting.jcw131_nyc_shooting_transformed")

# COMMAND ----------


