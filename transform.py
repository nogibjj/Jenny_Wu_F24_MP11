# Databricks notebook source
# Define a global variable for the log file
LOG_FILE = "query_log.md"


def log_query(query, result="none"):
    """adds to a query markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"```sql\n{query}\n```\n\n")
        file.write(f"```response from databricks\n{result}\n```\n\n")


def query(query: str, delta_table_name: str, table_name: str = None):
    try:
        # Check if delta_table_name is provided
        if not delta_table_name:
            raise ValueError(f"Delta table name must be provided, provided: {delta_table_name}")
        
        # If no table name is provided, extract from delta_table_name
        table_name = table_name or delta_table_name.split(".")[-1]
        
        
        table_name = f"`{table_name}`"  

        # No need to specify path if you're using Unity Catalog and Delta tables are registered
        # All of our IDS 706 tables are in this Unity Catalog
        log_query(query, result="Query received, executing next...")
        print(f"Executing SQL query on table {delta_table_name}")
        
        # Execute the query on the table
        result_df = spark.sql(query)
        
        pandas_df = result_df.toPandas()
        
        # Convert the Pandas DataFrame to a Markdown table so it can be seen in our query log
        result_str = pandas_df.to_markdown(index=False)  
        
        
        log_query(query, result=result_str)

        return result_df
    
    except Exception as e:
        # Catch and log any errors during the query execution
        error_message = f"Error occurred: {e}"
        log_query(query, result=error_message)
        print(error_message)
        return None 


    

# COMMAND ----------

# MAGIC %pip install tabulate
# MAGIC outputdf = query("""
# MAGIC     SELECT *
# MAGIC     FROM ids706_data_engineering.default.jcw131_nyc_shooting
# MAGIC     WHERE Occur_Date = '04/19/2008'
# MAGIC """, delta_table_name="ids706_data_engineering.default.jcw131_nyc_shooting")
# MAGIC
# MAGIC display(outputdf)
