# Databricks notebook source
# MAGIC %md
# MAGIC ### Code Setup

# COMMAND ----------

# Widget declarations allow interactive (non-job) runs without errors.
# In job context these are overridden by base_parameters / job-level parameters.
dbutils.widgets.text("write_location", "", "Destination volume path")
dbutils.widgets.text("load_location", "", "Source filename (relative to base_vol_load_path)")
dbutils.widgets.text("create_table_flag", "false", "Whether to write a UC table (true/false)")
dbutils.widgets.text("table_name", "NA", "UC table name (used when create_table_flag=true)")
# base_vol_load_path and catalog come from job-level parameters
dbutils.widgets.text("base_vol_load_path", "/Volumes/lr_nhl_demo/dev/logan_rupert/data/", "Source base path for CSV files (UC volume, no trailing .internal/)")
dbutils.widgets.text("catalog", "lr_nhl_demo.dev", "Fully-qualified catalog.schema (e.g. lr_nhl_demo.dev)")

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
    # Deduplicate schedule by (DATE, HOME, AWAY) for Lakebase sync primary key
    if table_name == "2025_26_official_nhl_schedule_by_day":
        df = df.dropDuplicates(["DATE", "HOME", "AWAY"])
    # catalog is "lr_nhl_demo.dev" or "lr_nhl_demo.prod" → "catalog.schema.table_name"
    df.write.mode("overwrite").saveAsTable(f"{catalog}.{table_name}")
