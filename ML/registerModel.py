# Databricks notebook source
# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
catalog = "lr_nhl_demo"
schema = "dev"
model_name = "bestSOGModel"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/2684982ac8854ad6a46a01526a0cdeba/model", f"{catalog}.{schema}.{model_name}")

# COMMAND ----------


