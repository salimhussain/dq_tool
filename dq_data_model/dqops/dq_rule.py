# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC Create a dqops table for dq rule

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA dqops;
# MAGIC --------------------------------------------------------------
# MAGIC -- Table Name: dq_rule
# MAGIC -- Description: Dimension table that stores information for each data quality rule
# MAGIC -- Granularity: One record per data quality rule
# MAGIC -- Loading method: Automatic - All new or updated rules are loaded from the data quality files for each stream
# MAGIC --------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS dq_rule (
# MAGIC   integration_id STRING GENERATED ALWAYS AS (table_name || '~' || column || '~' || CAST(rule_no AS STRING) || '~' || data_domain),
# MAGIC   rule_key BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
# MAGIC   rule_no INT NOT NULL,
# MAGIC   rule_description STRING NOT NULL,
# MAGIC   system_evaluated STRING NOT NULL,
# MAGIC   rule_priority STRING NOT NULL,
# MAGIC   start_date DATE NOT NULL,
# MAGIC   expiry_date DATE NOT NULL,
# MAGIC   rule_enabled STRING NOT NULL,
# MAGIC   data_domain STRING NOT NULL,
# MAGIC   data_product STRING NOT NULL,
# MAGIC   data_owner STRING NOT NULL,
# MAGIC   business_steward STRING NOT NULL,
# MAGIC   it_steward STRING NOT NULL,
# MAGIC   threshold FLOAT NOT NULL,
# MAGIC   weight FLOAT NOT NULL,
# MAGIC   rule_type_key INT NOT NULL,
# MAGIC   catalog STRING NOT NULL,
# MAGIC   schema STRING NOT NULL,
# MAGIC   table_name STRING NOT NULL,
# MAGIC   column STRING NOT NULL,
# MAGIC   table_key STRING NOT NULL,
# MAGIC   gx_arguments STRING NOT NULL,
# MAGIC   gx_conditions STRING,
# MAGIC   created_at_ts TIMESTAMP NOT NULL,
# MAGIC   modified_at_ts TIMESTAMP NOT NULL,
# MAGIC   CONSTRAINT pk_dq_rule PRIMARY KEY (integration_id, rule_key)
# MAGIC ) USING DELTA;

# COMMAND ----------
