# Databricks notebook source
# MAGIC %md
# MAGIC ### Code Setup

# COMMAND ----------

write_location = dbutils.widgets.get("write_location")
load_location = dbutils.widgets.get("load_location")
create_table_flag = dbutils.widgets.get("create_table_flag")
base_vol_load_path = dbutils.widgets.get("base_vol_load_path")

if create_table_flag == "false":
    dbutils.fs.cp(base_vol_load_path + load_location, write_location)
    print(f"Files uploaded to volume: {write_location}")

if create_table_flag == "true":
    catalog = dbutils.widgets.get("catalog")
    table_name = dbutils.widgets.get("table_name")

    # Create DataFrame and write to UC table
    df = spark.read.option("header", True).csv(base_vol_load_path + load_location)
    df.write.saveAsTable(f"{catalog}.{table_name}")
