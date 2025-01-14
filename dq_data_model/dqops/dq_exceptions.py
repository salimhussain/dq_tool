# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC Create a dqops table for dq exceptions

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table dqops.dq_exceptions

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table dqops.dq_exceptions

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA dqops;
# MAGIC CREATE TABLE IF NOT EXISTS dq_exceptions (
# MAGIC   integration_id STRING GENERATED ALWAYS AS (exception_row_pk || '~' || CAST(created_at_ts AS STRING)),
# MAGIC   exception_key BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   validation_key BIGINT NOT NULL,
# MAGIC   rule_key BIGINT NOT NULL,
# MAGIC   execution_key INT NOT NULL,
# MAGIC   exception_row_pk STRING NOT NULL,
# MAGIC   exception_row_value STRING NOT NULL,
# MAGIC   fixed_exception_flag BOOLEAN NOT NULL,
# MAGIC   new_exception_flag BOOLEAN NOT NULL,
# MAGIC   row_delete_flag BOOLEAN NOT NULL,
# MAGIC   created_at_ts TIMESTAMP NOT NULL,
# MAGIC   modified_at_ts TIMESTAMP NOT NULL,
# MAGIC   CONSTRAINT pk_dq_exceptions PRIMARY KEY (integration_id, exception_key)
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable feature for column defaults
# MAGIC ALTER TABLE dq_exceptions SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC -- Enable default value current_timestamp() for created_at_ts and modified_at_ts
# MAGIC ALTER TABLE dq_exceptions ALTER COLUMN created_at_ts SET DEFAULT current_timestamp();
# MAGIC ALTER TABLE dq_exceptions ALTER COLUMN modified_at_ts SET DEFAULT current_timestamp();

# COMMAND ----------


