# Databricks notebook source
# Import packages
import requests
import os
import zipfile
import chardet
import io
import pandas as pd
from pyspark.sql import SparkSession

def extract_load(
    url,
    catalog_name,
    schema_name,
    table_name
):
    """Extract to file path"""
    spark = SparkSession.builder.appName("Read CSV from URL").getOrCreate()

    df = pd.read_csv(url, sep=",", encoding="utf-8")
    display(df)
    nyc_shooting_df = spark.createDataFrame(df)

    nyc_shooting_df.write.format("delta") \
        .option("mergeSchema", "true") \
        .mode("overwrite") \
        .saveAsTable((f"{catalog_name}.{schema_name}.{table_name}"))
    print("Dataframe saved to table")

# COMMAND ----------

extract_load(
      url="https://raw.githubusercontent.com/nogibjj/Jenny_Wu_F24_MP5/refs/heads/main/data/nypd_shooting.csv?", catalog_name = "ids706_data_engineering", schema_name = "jcw131_nyc_shooting", table_name = "jcw131_nyc_shooting",
    )
