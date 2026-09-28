# Databricks notebook source
volume_folder = '/Volumes/databricks_lakehouse_connect/databricks_lakehouse/data'

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog databricks_lakehouse_connect

# COMMAND ----------

display(dbutils.fs.ls(volume_folder))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM read_files('{volume_folder}/netflix_titles.csv') LIMIT 5"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE databricks_lakehouse_connect.databricks_lakehouse.netflix_titles_CTAS AS
# MAGIC           SELECT *
# MAGIC           FROM read_files(
# MAGIC             'dbfs:/Volumes/databricks_lakehouse_connect/databricks_lakehouse/data/netflix_titles.csv',
# MAGIC             format => 'csv'
# MAGIC           );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE databricks_lakehouse_connect.databricks_lakehouse.netflix_titles_copy_into (id INT, title STRING, genre STRING, release_year INT) USING DELTA;
# MAGIC         
# MAGIC COPY INTO databricks_lakehouse_connect.databricks_lakehouse.netflix_titles_copy_into
# MAGIC FROM 'dbfs:/Volumes/databricks_lakehouse_connect/databricks_lakehouse/data/netflix_titles.csv'
# MAGIC FILEFORMAT = CSV
# MAGIC FORMAT_OPTIONS ('header' = 'true',
# MAGIC                 'inferSchema' = 'false',
# MAGIC                 'delimiter' = ',',
# MAGIC                 'nullValue' = 'NULL')
# MAGIC COPY_OPTIONS ('overwriteSchema' = 'true', 'OVERWRITE' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_lakehouse_connect.databricks_lakehouse.netflix_titles_copy_into

# COMMAND ----------


source_volume_path = "dbfs:/Volumes/databricks_lakehouse_connect/databricks_lakehouse/data/*.json"
target_table       = "databricks_lakehouse_connect.databricks_lakehouse.nobel_prizes_autoloader"

checkpoint_base_dir = "dbfs:/Volumes/databricks_lakehouse_connect/databricks_lakehouse/data/autoloader/"
schema_checkpoint   = f"{checkpoint_base_dir}/autoloader_schema"
stream_checkpoint   = f"{checkpoint_base_dir}/stream_state"

def start_autoloader_stream():
    
    df_raw = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_checkpoint) 
        .option("multiLine", "true")
        .option("cloudFiles.inferColumnTypes", "true") 
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") 
        .load(source_volume_path)
    )
    

    streaming_query = (df_raw.writeStream
        .format("delta")
        .option("checkpointLocation", stream_checkpoint)
        .option("mergeSchema", "true")
        .trigger(processingTime="10 seconds")
        .toTable(target_table)
    )
    
    return streaming_query

query = start_autoloader_stream()

# COMMAND ----------

df = spark.read \
  .format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .load("dbfs:/Volumes/databricks_lakehouse_connect/databricks_lakehouse/data/Online_Retail.csv") \
  .select("*","_metadata.file_modification_time")
df.display()

# COMMAND ----------

df = spark.read \
  .format("csv") \
  .option("header", "true") \
  .option("inferSchema", "true") \
  .option("rescuedDataColumn", "_rescued_data") \
  .load("dbfs:/Volumes/databricks_lakehouse_connect/databricks_lakehouse/data/Online_Retail.csv") \
  .select("*")
df.display()
# salary : int/double = 100, 1000 => "1000"


# COMMAND ----------

