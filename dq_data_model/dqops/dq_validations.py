# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC Create a gold table for fact dq validations

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA dqops;
# MAGIC CREATE TABLE dq_validations (
# MAGIC   integration_id STRING GENERATED ALWAYS AS (rule_key || '~' || execution_key),
# MAGIC   validation_key BIGINT NOT NULL,
# MAGIC   rule_key BIGINT NOT NULL,
# MAGIC   execution_key INT NOT NULL,
# MAGIC   success_flag BOOLEAN NOT NULL,
# MAGIC   element_count INTEGER NOT NULL,
# MAGIC   exception_count INTEGER NOT NULL,
# MAGIC   known_exception_count INTEGER NOT NULL,
# MAGIC   new_exception_count INTEGER NOT NULL,
# MAGIC   fixed_exception_count INTEGER NOT NULL,
# MAGIC   exception_percent FLOAT NOT NULL,
# MAGIC   known_exception_percent FLOAT NOT NULL,
# MAGIC   new_exception_percent FLOAT NOT NULL,
# MAGIC   fixed_exception_percent FLOAT NOT NULL,
# MAGIC   observed_value STRING,
# MAGIC   missing_count INTEGER NOT NULL,
# MAGIC   missing_percent FLOAT NOT NULL,
# MAGIC   exception_percent_total FLOAT NOT NULL,
# MAGIC   exception_percent_nonmissing FLOAT NOT NULL,
# MAGIC   created_at_ts TIMESTAMP NOT NULL,
# MAGIC   modified_at_ts TIMESTAMP NOT NULL,
# MAGIC   CONSTRAINT dq_validations PRIMARY KEY (integration_id, validation_key)
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable the Delta feature for column defaults
# MAGIC ALTER TABLE dq_validations SET TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'enabled');
# MAGIC -- Set default value current_timestamp() for column modified_at_ts and created_at_ts
# MAGIC ALTER TABLE dq_validations ALTER COLUMN modified_at_ts SET DEFAULT current_timestamp();
# MAGIC ALTER TABLE dq_validations ALTER COLUMN created_at_ts SET DEFAULT current_timestamp();

# COMMAND ----------
