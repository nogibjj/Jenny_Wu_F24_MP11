# Jenny_Wu_F24_MP11

![](/Workspace/Shared/Jenny_Wu_F24_MP11/successful_pipeline)

#### Purpose of Project
This project demonstrates the implementation of an **ETL (Extract, Transform, Load) and Query pipeline within the Databricks environment**. The pipeline is developed using **PySpark**, extracting raw data, applying transformations, and storing the processed data in Delta Tables for efficient querying and analysis.

## Project Overview

The ETL-Query pipeline includes the following steps:
1. **Extract**: Raw data is extracted from online data source and saved in local file path. In this project, we loaded in a dataset the recorded all NYPD shooting incidents. 
2. **Load**: Both the raw data and the transformed data are stored in **Delta Table** within the Databricks environment and into the IDS 703 catalogue. 
3. **Transform**: Data cleaning, formatting, and enrichment processes are applied using **PySpark** to make the data analysis-ready. To transform the data, we filtered the dataset to just include all incidents which occurred in Queens. We conducted another load to save this transformed database into a Delta Table. 
4. **Query**: The Delta Table serves as the foundation for running **SQL** queries and performing analysis efficiently. For this project, we queried all incidents that occurred on April 19th, 2008. These results can be found in the query_log.md file. 



