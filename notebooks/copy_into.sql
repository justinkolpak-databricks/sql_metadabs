-- Databricks notebook source
USE CATALOG '${tgt_catalog}';
CREATE SCHEMA IF NOT EXISTS IDENTIFIER('${tgt_schema}');
USE IDENTIFIER(CONCAT('${tgt_catalog}','.', '${tgt_schema}')); -- this is the only combo that work at schema level. USE SCHEMA does not like dynamic

-- COMMAND ----------

-- DBTITLE 1,Confirm Scope
SELECT current_catalog(), current_database()

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE full_tgt_table_name STRING;
SET VAR full_tgt_table_name = :tgt_catalog || '.' || :tgt_schema || '.' || :tgt_table;

EXECUTE IMMEDIATE "CREATE TABLE IF NOT EXISTS IDENTIFIER(full_tgt_table_name)
TBLPROPERTIES('delta.enableChangeDataFeed'='true')
"

-- COMMAND ----------

DECLARE OR REPLACE VARIABLE copy_into_sql STRING;
SET VAR copy_into_sql = "COPY INTO " || full_tgt_table_name || "
FROM (SELECT 
      *
      , now() AS _etl_timestamp
FROM '" || :src_path || "')
FILEFORMAT = "|| :src_format || "
COPY_OPTIONS('mergeSchema' = 'true')";

SELECT copy_into_sql;

EXECUTE IMMEDIATE copy_into_sql

-- COMMAND ----------

-- One generic table to track all the tables in the CDF
CREATE TABLE IF NOT EXISTS cdf_checkpoint_events (full_table_name STRING, latest_version INT DEFAULT 0, update_timestamp TIMESTAMP)
TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
