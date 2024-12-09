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
    filepath,
):
    """Extract to file path"""
    spark = SparkSession.builder.appName("Read CSV from URL").getOrCreate()

    df = pd.read_csv(url, sep=",", encoding="utf-8")
    display(df)
    nyc_shooting_df = spark.createDataFrame(df)
    nyc_shooting_df.write.format("delta").mode("append").saveAsTable("jcw131_nyc_shooting")
    print("Dataframe saved to table")
    return filepath

# COMMAND ----------


extract_load(
      url="https://raw.githubusercontent.com/nogibjj/Jenny_Wu_F24_MP5/refs/heads/main/data/nypd_shooting.csv?", filepath="data/jcw131_nyc_shooting.csv",
    )
