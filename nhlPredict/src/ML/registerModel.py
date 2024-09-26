# Databricks notebook source
# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

best_model_uri_task = dbutils.jobs.taskValues.get(
    taskKey="get_model_uri", key="best_model_uri", debugValue="some_value"
)
print(best_model_uri_task)

# COMMAND ----------

import mlflow

# catalog = "lr_nhl_demo"
# schema = "dev"
# model_name = "SOGModel_v2"
# mlflow.set_registry_uri("databricks-uc")
# # mlflow.register_model("runs:/6667e97b3600461d827eebf069c9b6e3/model", f"{catalog}.{schema}.{model_name}")
# # mlflow.register_model("runs:/4e3dd50a456d4687bf06c214452aebc4/model", f"{catalog}.{schema}.{model_name}") df72db8230f04d28ad2bd1231323aeee
# mlflow.register_model(
#     model_uri=best_model_uri_task, name=f"{catalog}.{schema}.{model_name}"
# )

# COMMAND ----------

# Set as Champion based on max version
client = mlflow.tracking.MlflowClient()
model_version_infos = client.search_model_versions(
    "name = 'lr_nhl_demo.dev.player_prediction_sog'"
)
new_model_version = max(
    [model_version_info.version for model_version_info in model_version_infos]
)
client.set_registered_model_alias(
    "lr_nhl_demo.dev.player_prediction_sog", "champion", new_model_version
)

# COMMAND ----------


