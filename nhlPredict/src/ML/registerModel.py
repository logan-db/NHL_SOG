# Databricks notebook source
# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------
import mlflow
from mlflow.tracking import MlflowClient


def get_best_run(experiment_id, metric_name):
    client = MlflowClient()
    runs = client.search_runs(
        experiment_id, order_by=[f"metrics.{metric_name} ASC"], max_results=1
    )
    return runs[0]


# Example usage
experiment_id = "634720160613016"
metric_name = "training_mean_absolute_error"
best_run = get_best_run(experiment_id, metric_name)
print(f"Best run ID: {best_run.info.run_id}")
print(f"Best run {metric_name}: {best_run.data.metrics[metric_name]}")

# COMMAND ----------

# model = best_run["model"]
# mlflow_run = best_run["run"]

# display(
#     pd.DataFrame(
#         [best_result["val_metrics"], best_result["test_metrics"]],
#         index=["validation", "test"],
#     )
# )

# set_config(display="diagram")
# model

# COMMAND ----------

# Assuming you have the best_run_id from the previous experiment
best_run_id = best_run.info.run_id

# Load the best model's configuration
best_run = mlflow.get_run(best_run_id)
best_params = best_run.data.params

# best_params = {k: v for k, v in best_run.data.params.items()}
best_model_type = best_run.data.tags["model_type"]

# COMMAND ----------

# with mlflow.start_run():

#   # df has columns ['customer_id', 'product_id', 'rating']
#   training_set = fe.create_training_set(
#     df=df,
#     feature_lookups=feature_lookups,
#     label='rating',
#     exclude_columns=['customer_id', 'product_id']
#   )

#   training_df = training_set.load_df().toPandas()

#   # "training_df" columns ['total_purchases_30d', 'category', 'rating']
#   X_train = training_df.drop(['rating'], axis=1)
#   y_train = training_df.rating

#   model = linear_model.LinearRegression().fit(X_train, y_train)

#   fe.log_model(
#     model=model,
#     artifact_path="recommendation_model",
#     flavor=mlflow.sklearn,
#     training_set=training_set,
#     registered_model_name="recommendation_model"
#   )

# # Batch inference

# # If the model at model_uri is packaged with the features, the FeatureStoreClient.score_batch()
# # call automatically retrieves the required features from Feature Store before scoring the model.
# # The DataFrame returned by score_batch() augments batch_df with
# # columns containing the feature values and a column containing model predictions.

# fe = FeatureEngineeringClient()

# # batch_df has columns ‘customer_id’ and ‘product_id’
# predictions = fe.score_batch(
#     model_uri=model_uri,
#     df=batch_df
# )

# # The ‘predictions’ DataFrame has these columns:
# # ‘customer_id’, ‘product_id’, ‘total_purchases_30d’, ‘category’, ‘prediction’


# COMMAND ----------

best_params["model_type"] = best_model_type
result = objective(best_params, X, y, full_train_flag=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Patch pandas version in logged model
# MAGIC
# MAGIC Ensures that model serving uses the same version of pandas that was used to train the model.

# COMMAND ----------

import mlflow
import os
import shutil
import tempfile
import yaml

run_id = mlflow_run.info.run_id

# Set up a local dir for downloading the artifacts.
tmp_dir = tempfile.mkdtemp()

client = mlflow.tracking.MlflowClient()

# Fix conda.yaml
conda_file_path = mlflow.artifacts.download_artifacts(
    artifact_uri=f"runs:/{run_id}/model/conda.yaml", dst_path=tmp_dir
)
with open(conda_file_path) as f:
    conda_libs = yaml.load(f, Loader=yaml.FullLoader)
pandas_lib_exists = any(
    [lib.startswith("pandas==") for lib in conda_libs["dependencies"][-1]["pip"]]
)
if not pandas_lib_exists:
    print("Adding pandas dependency to conda.yaml")
    conda_libs["dependencies"][-1]["pip"].append(f"pandas=={pd.__version__}")

    with open(f"{tmp_dir}/conda.yaml", "w") as f:
        f.write(yaml.dump(conda_libs))
    client.log_artifact(
        run_id=run_id, local_path=conda_file_path, artifact_path="model"
    )

# Fix requirements.txt
venv_file_path = mlflow.artifacts.download_artifacts(
    artifact_uri=f"runs:/{run_id}/model/requirements.txt", dst_path=tmp_dir
)
with open(venv_file_path) as f:
    venv_libs = f.readlines()
venv_libs = [lib.strip() for lib in venv_libs]
pandas_lib_exists = any([lib.startswith("pandas==") for lib in venv_libs])
if not pandas_lib_exists:
    print("Adding pandas dependency to requirements.txt")
    venv_libs.append(f"pandas=={pd.__version__}")

    with open(f"{tmp_dir}/requirements.txt", "w") as f:
        f.write("\n".join(venv_libs))
    client.log_artifact(run_id=run_id, local_path=venv_file_path, artifact_path="model")

shutil.rmtree(tmp_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature importance
# MAGIC
# MAGIC SHAP is a game-theoretic approach to explain machine learning models, providing a summary plot
# MAGIC of the relationship between features and model output. Features are ranked in descending order of
# MAGIC importance, and impact/color describe the correlation between the feature and the target variable.
# MAGIC - Generating SHAP feature importance is a very memory intensive operation, so to ensure that AutoML can run trials without
# MAGIC   running out of memory, we disable SHAP by default.<br />
# MAGIC   You can set the flag defined below to `shap_enabled = True` and re-run this notebook to see the SHAP plots.
# MAGIC - To reduce the computational overhead of each trial, a single example is sampled from the validation set to explain.<br />
# MAGIC   For more thorough results, increase the sample size of explanations, or provide your own examples to explain.
# MAGIC - SHAP cannot explain models using data with nulls; if your dataset has any, both the background data and
# MAGIC   examples to explain will be imputed using the mode (most frequent values). This affects the computed
# MAGIC   SHAP values, as the imputed samples may not match the actual data distribution.
# MAGIC
# MAGIC For more information on how to read Shapley values, see the [SHAP documentation](https://shap.readthedocs.io/en/latest/example_notebooks/overviews/An%20introduction%20to%20explainable%20AI%20with%20Shapley%20values.html).
# MAGIC
# MAGIC > **NOTE:** SHAP run may take a long time with the datetime columns in the dataset.

# COMMAND ----------

# Set this flag to True and re-run the notebook to see the SHAP plots
shap_enabled = True

# COMMAND ----------

if shap_enabled:
    mlflow.autolog(disable=True)
    mlflow.sklearn.autolog(disable=True)
    from shap import KernelExplainer, summary_plot

    # SHAP cannot explain models using data with nulls.
    # To enable SHAP to succeed, both the background data and examples to explain are imputed with the mode (most frequent values).
    mode = X_train_processed.mode().iloc[0]

    # Sample background data for SHAP Explainer. Increase the sample size to reduce variance.
    train_sample = X_train_processed.sample(
        n=min(1000, X_train_processed.shape[0]), random_state=729986891
    ).fillna(mode)

    # Sample some rows from the validation set to explain. Increase the sample size for more thorough results.
    example = X_val_processed.sample(
        n=min(1000, X_val_processed.shape[0]), random_state=729986891
    ).fillna(mode)

    # Use Kernel SHAP to explain feature importance on the sampled rows from the validation set.
    predict = lambda x: model.predict(
        pd.DataFrame(x, columns=X_train_processed.columns)
    )
    explainer = KernelExplainer(predict, train_sample, link="identity")
    shap_values = explainer.shap_values(example, l1_reg=False, nsamples=1000)
    summary_plot(shap_values, example)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inference
# MAGIC [The MLflow Model Registry](https://docs.databricks.com/applications/mlflow/model-registry.html) is a collaborative hub where teams can share ML models, work together from experimentation to online testing and production, integrate with approval and governance workflows, and monitor ML deployments and their performance. The snippets below show how to add the model trained in this notebook to the model registry and to retrieve it later for inference.
# MAGIC
# MAGIC > **NOTE:** The `model_uri` for the model already trained in this notebook can be found in the cell below
# MAGIC
# MAGIC ### Register to Model Registry
# MAGIC ```
# MAGIC model_name = "Example"
# MAGIC
# MAGIC model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
# MAGIC registered_model_version = mlflow.register_model(model_uri, model_name)
# MAGIC ```
# MAGIC
# MAGIC ### Load from Model Registry
# MAGIC ```
# MAGIC model_name = "Example"
# MAGIC model_version = registered_model_version.version
# MAGIC
# MAGIC model_uri=f"models:/{model_name}/{model_version}"
# MAGIC model = mlflow.pyfunc.load_model(model_uri=model_uri)
# MAGIC model.predict(input_X)
# MAGIC ```
# MAGIC
# MAGIC ### Load model without registering
# MAGIC ```
# MAGIC model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
# MAGIC
# MAGIC model = mlflow.pyfunc.load_model(model_uri=model_uri)
# MAGIC model.predict(input_X)
# MAGIC ```

# COMMAND ----------

# model_uri for the generated model
best_model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
print(best_model_uri)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="best_model_uri", value=best_model_uri)
print(f"Successfully set the best_model_uri to {best_model_uri}")
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
    f"name = '{catalog_param}.player_prediction_sog'"
)
new_model_version = max(
    [model_version_info.version for model_version_info in model_version_infos]
)
client.set_registered_model_alias(
    f"{catalog_param}.player_prediction_sog", "champion", new_model_version
)

# COMMAND ----------
