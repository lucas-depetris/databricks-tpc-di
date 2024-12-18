-- Databricks notebook source
-- DBTITLE 1,Define and get query parameters
-- MAGIC %python
-- MAGIC # Define widgets with default values (will be overridden by workflow parameters)
-- MAGIC dbutils.widgets.text('catalog', 'tpcdi')
-- MAGIC dbutils.widgets.text('wh_db', 'default')
-- MAGIC dbutils.widgets.text('scale_factor', '10')
-- MAGIC dbutils.widgets.text('tbl', '')
-- MAGIC dbutils.widgets.text('tbl_props', '')
-- MAGIC dbutils.widgets.text('raw_schema', '')
-- MAGIC dbutils.widgets.text('constraints', '')
-- MAGIC dbutils.widgets.text('tpcdi_directory', '')
-- MAGIC dbutils.widgets.text('filename', '')
-- MAGIC
-- MAGIC # Get query parameter values (from workflow or widget UI)
-- MAGIC catalog = dbutils.widgets.get('catalog')
-- MAGIC wh_db = dbutils.widgets.get('wh_db')
-- MAGIC scale_factor = dbutils.widgets.get('scale_factor')
-- MAGIC tbl = dbutils.widgets.get('tbl')
-- MAGIC tblprops = dbutils.widgets.get('tbl_props')
-- MAGIC raw_schema = dbutils.widgets.get('raw_schema')
-- MAGIC constraints = dbutils.widgets.get('constraints')
-- MAGIC tpcdi_directory = dbutils.widgets.get('tpcdi_directory')
-- MAGIC filename = dbutils.widgets.get('filename')
-- MAGIC
-- MAGIC # Set these as SQL query parameters for use in subsequent SQL cells
-- MAGIC spark.conf.set('catalog', catalog)
-- MAGIC spark.conf.set('wh_db', wh_db)
-- MAGIC spark.conf.set('scale_factor', scale_factor)
-- MAGIC spark.conf.set('tbl', tbl)
-- MAGIC spark.conf.set('tpcdi_directory', tpcdi_directory)
-- MAGIC spark.conf.set('filename', filename)
-- MAGIC spark.conf.set('raw_schema', raw_schema)
-- MAGIC
-- MAGIC print(f"Table: {catalog}.{wh_db}_{scale_factor}.{tbl}")

-- COMMAND ----------

-- DBTITLE 1,Construct and execute CREATE TABLE
-- MAGIC %python
-- MAGIC # Construct the full table name
-- MAGIC table_name = f"{catalog}.{wh_db}_{scale_factor}.{tbl}"
-- MAGIC
-- MAGIC # Build the schema string with batchid column
-- MAGIC schema_str = f"{raw_schema}, batchid INT COMMENT 'Batch ID when this record was inserted' {constraints}"
-- MAGIC
-- MAGIC # Build the complete SQL statement
-- MAGIC sql_stmt = f"""
-- MAGIC CREATE OR REPLACE TABLE {table_name} (
-- MAGIC   {schema_str}
-- MAGIC )
-- MAGIC TBLPROPERTIES ({tblprops})
-- MAGIC """
-- MAGIC
-- MAGIC print("Executing SQL:")
-- MAGIC print(sql_stmt)
-- MAGIC
-- MAGIC # Execute the SQL statement
-- MAGIC spark.sql(sql_stmt)

-- COMMAND ----------

INSERT OVERWRITE IDENTIFIER(:catalog || '.' || :wh_db || '_' || :scale_factor || '.' || :tbl)
SELECT
  *,
  int(substring(_metadata.file_path FROM (position('/Batch', _metadata.file_path) + 6) FOR 1)) batchid
FROM read_files(
  :tpcdi_directory || 'sf=' || :scale_factor || '/Batch*',
  format => "csv",
  inferSchema => False,
  header => False,
  sep => "|",
  schemaEvolutionMode => 'none',
  fileNamePattern => :filename,
  schema => :raw_schema
)