# Databricks notebook source
# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
catalog = "lr_nhl_demo"
schema = "dev"
model_name = "SOGModel_v2"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/6667e97b3600461d827eebf069c9b6e3/model", f"{catalog}.{schema}.{model_name}")

# COMMAND ----------


