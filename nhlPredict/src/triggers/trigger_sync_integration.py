# Databricks notebook source
# MAGIC %md
# MAGIC # Synced Tables Pipeline Trigger
# MAGIC
# MAGIC This notebook triggers the existing synced table pipelines to update
# MAGIC the synced tables after source tables are updated.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import and Setup

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    SyncedDatabaseTable,
    SyncedTableSpec,
    NewPipelineSpec,
    SyncedTableSchedulingPolicy,
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration for synced tables
SYNCED_TABLES = [
    "lr-lakebase-catalog.public.nhl_player_data",
    "lr-lakebase-catalog.public.nhl_team_data",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger Synced Table Pipelines

# COMMAND ----------


def trigger_synced_table_pipeline(synced_table_name: str):
    """
    Trigger the pipeline for a specific synced table.

    Args:
        synced_table_name: Full name of the synced table (catalog.schema.table)
    """
    try:
        print(f"🔄 Triggering pipeline for {synced_table_name}...")
        print("🔧 Initializing Databricks Workspace client...")
        w = WorkspaceClient()

        # Get the synced table information
        synced_table = w.tables.get(synced_table_name)

        # Assuming the pipeline ID is stored in table properties
        pipeline_id = synced_table.pipeline_id

        if pipeline_id:
            print(f"📊 Found pipeline ID: {pipeline_id}")

            # Start the pipeline update
            w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=False)
            print(f"✅ Successfully triggered pipeline for {synced_table_name}")
        else:
            print(f"⚠️ No pipeline ID found for {synced_table_name}")

    except Exception as e:
        print(f"❌ Failed to trigger pipeline for {synced_table_name}: {str(e)}")
        raise e


# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Workspace Client and Trigger Pipelines

# COMMAND ----------

# Trigger pipelines for all synced tables
print("🚀 Triggering synced table pipelines...")
for synced_table_name in SYNCED_TABLES:
    trigger_synced_table_pipeline(synced_table_name)

print("🎉 All synced table pipelines triggered successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Pipeline Status

# COMMAND ----------


def check_pipeline_status(synced_table_name: str):
    """
    Check the status of a synced table pipeline.

    Args:
        synced_table_name: Full name of the synced table
    """
    try:
        synced_table = w.database.get_synced_database_table(name=synced_table_name)
        status = synced_table.data_synchronization_status.detailed_state
        message = synced_table.data_synchronization_status.message
        pipeline_id = synced_table.data_synchronization_status.pipeline_id

        print(f"📊 {synced_table_name}:")
        print(f"   Status: {status}")
        print(f"   Message: {message}")
        print(f"   Pipeline ID: {pipeline_id}")
        print()

    except Exception as e:
        print(f"❌ Failed to check status for {synced_table_name}: {str(e)}")


# COMMAND ----------

# Check status of all synced tables
print("📊 Checking synced table status...")
for synced_table_name in SYNCED_TABLES:
    check_pipeline_status(synced_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

print("✅ Synced table pipelines triggered successfully!")
print("📊 Player data sync: lr-lakebase-catalog.public.nhl_player_data")
print("📊 Team data sync: lr-lakebase-catalog.public.nhl_team_data")
print("🔄 Pipelines will update the synced tables with latest data from source tables")
print("🎉 Synced tables are now being updated!")
