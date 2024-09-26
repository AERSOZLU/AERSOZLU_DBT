# Databricks notebook source
# MAGIC %sql
# MAGIC list  "abfss://momentum@databricksdap2poc.dfs.core.windows.net"
# MAGIC --<path to source data>: The path to a file in the directory that contains your data.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC list  "abfss://momentum@databricksdap2poc.dfs.core.windows.net/de/"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from read_files('abfss://momentum@databricksdap2poc.dfs.core.windows.net/de/customer_grouping.parquet', format => 'parquet' )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE momentum.stage.customer_grouping as SELECT * FROM  read_files('abfss://momentum@databricksdap2poc.dfs.core.windows.net/de/customer_grouping.parquet', format => 'parquet' )
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from momentum.stage.customer_grouping
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE momentum.stage.customer_grouping
# MAGIC (
# MAGIC column1 int
# MAGIC column2 Varchar(32)
# MAGIC :
# MAGIC ) ;

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO momentum.stage.customer_grouping
# MAGIC FROM 'abfss://momentum@databricksdap2poc.dfs.core.windows.net/de/customer_grouping.parquet'
# MAGIC FILEFORMAT= parquet
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC set table_name= '"DB.SCHEMA.TABLE"';
# MAGIC SET sqlQuery = concat('DESC TABLE ', $table_name,';');
# MAGIC
# MAGIC EXECUTE IMMEDIATE $sqlQuery;
# MAGIC
# MAGIC set desc_table_qid = LAST_QUERY_ID();
# MAGIC select ' CREATE OR REPLACE VIEW DB.SCHEMA.TABLE ('    ||
# MAGIC ARRAY_TO_STRING(ARRAY_AGG(
# MAGIC     concat_ws(',',LOWER(
# MAGIC     CONCAT('"',REGEXP_REPLACE(REGEXP_REPLACE(t."comment",'[ -]','_'),'[+,%().]',''),'"',' COMMENT ',''''
# MAGIC     ,t."name",'''')
# MAGIC         ))
# MAGIC     ),',') || ')AS SELECT ' || ARRAY_TO_STRING(ARRAY_AGG(
# MAGIC     concat_ws(',',concat('"',t."name",'" as ',LOWER(REGEXP_REPLACE(REGEXP_REPLACE(t."comment",'[ -]','_'),'[+,%,().]',''))))),',') || ' from ' || $table_name
# MAGIC from TABLE(RESULT_SCAN($desc_table_qid)) as t
# MAGIC
# MAGIC ##DESC TABLE "DB"."SCHEMA"."TABLE"
