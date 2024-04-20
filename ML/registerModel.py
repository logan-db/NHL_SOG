# Databricks notebook source
# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
catalog = "lr_nhl_demo"
schema = "dev"
model_name = "SOGModel"
mlflow.set_registry_uri("databricks-uc")
mlflow.register_model("runs:/6834dfcb3b0640dab3c8cf80b9e9f61c/model", f"{catalog}.{schema}.{model_name}")

# COMMAND ----------


