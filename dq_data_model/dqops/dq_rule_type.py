# Databricks notebook source
# MAGIC %md
# MAGIC # Overview
# MAGIC Create a dqops table for dq rule type

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA dqops;
# MAGIC --------------------------------------------------------------
# MAGIC -- Table Name: dq_rule_type
# MAGIC -- Description: Dimension table that stores information for each data quality rule type
# MAGIC -- Granularity: One record per data quality rule type
# MAGIC -- Loading method: Automatic - All new or updated rules are loaded from Sharepoint
# MAGIC --------------------------------------------------------------
# MAGIC CREATE TABLE IF NOT EXISTS dq_rule_type (
# MAGIC   integration_id STRING GENERATED ALWAYS AS ((CAST(rule_type_key AS STRING)) || '~' || gx_expectation),
# MAGIC   rule_type_key INT NOT NULL,
# MAGIC   rule_type_name STRING NOT NULL,
# MAGIC   dq_dimension STRING NOT NULL,
# MAGIC   gx_expectation STRING NOT NULL,
# MAGIC   gx_description STRING NOT NULL,
# MAGIC   created_at_ts TIMESTAMP NOT NULL,
# MAGIC   modified_at_ts TIMESTAMP NOT NULL,
# MAGIC   CONSTRAINT pk_dq_rule_type PRIMARY KEY (integration_id, rule_type_key)
# MAGIC ) USING DELTA;
