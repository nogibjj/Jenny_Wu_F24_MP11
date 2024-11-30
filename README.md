# Jenny_Wu_F24_MP11

#### Purpose of Project
This project demonstrates the implementation of an **ETL (Extract, Transform, Load) and Query pipeline within the Databricks environment**. The pipeline is developed using **PySpark**, extracting raw data, applying transformations, and storing the processed data in Delta Tables for efficient querying and analysis.

## Project Overview

The ETL-Query pipeline includes the following steps:
1. **Extract**: Raw data is extracted from online data source and saved in local file path.
2. **Transform**: Data cleaning, formatting, and enrichment processes are applied using **PySpark** to make the data analysis-ready.
3. **Load**: Both the raw data and the transformed data are stored in **Delta Table** within the Databricks environment.
4. **Query**: The Delta Table serves as the foundation for running **SQL** queries and performing analysis efficiently.

