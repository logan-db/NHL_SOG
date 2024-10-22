# Databricks notebook source
# MAGIC %md
# MAGIC ## Get Best Trial Model

# COMMAND ----------

# MAGIC %pip install "mlflow-skinny[databricks]>=2.4.1"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Create widgets with default values
dbutils.widgets.text("catalog", "lr_nhl_demo.dev", "Catalog name")
dbutils.widgets.text("target_col", "player_Total_shotsOnGoal", "target_col")
dbutils.widgets.text("trial_eval_param", "10", "trial_eval_param")
dbutils.widgets.text("trial_experiment_param", "4320825364109465", "trial_experiment_param")
dbutils.widgets.text("training_experiment_param", "4320825364109641", "training_experiment_param")

# COMMAND ----------

# TESTING
feature_count_param = 100

# COMMAND ----------

trial_experiment_param = str(dbutils.widgets.get("trial_experiment_param"))
training_experiment_param = str(dbutils.widgets.get("training_experiment_param"))

target_col = dbutils.widgets.get("target_col")
id_columns = ["gameId", "playerId"]

trial_eval_param = int(dbutils.widgets.get("trial_eval_param"))
catalog_param = dbutils.widgets.get("catalog").lower()

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient

def get_best_run(experiment_id, metric_name):
    client = MlflowClient()
    runs = client.search_runs(
        experiment_id, order_by=[f"metrics.{metric_name} DESC"], max_results=1
    )
    return runs[0]


metric_name = "test_r2_score"
best_run = get_best_run(trial_experiment_param, metric_name)
print(f"Best run ID: {best_run.info.run_id}")
print(f"Best run {metric_name}: {best_run.data.metrics[metric_name]}")
print(f"Best run MSE: {best_run.data.metrics['test_mean_squared_error']}")

# COMMAND ----------

# Assuming you have the best_run_id from the previous experiment
best_run_id = best_run.info.run_id
best_model_type = best_run.data.tags.get("model_type", None)

# Load the best model's configuration
best_run = mlflow.get_run(best_run_id)
best_params = best_run.data.params

print(f"best_model_type: {best_model_type}")
best_params

# COMMAND ----------

feature_count_param = int(best_run.data.tags.get("features_count", None))
feature_count_param

# COMMAND ----------

# MAGIC %md
# MAGIC ## Patch pandas version in logged model
# MAGIC
# MAGIC Ensures that model serving uses the same version of pandas that was used to train the model.

# COMMAND ----------

import mlflow
import os
import shutil
import tempfile
import yaml

run_id = best_run_id

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
# MAGIC ## RETRAIN best model trial on FULL dataset

# COMMAND ----------

# Load f"{catalog_param}.pre_feat_eng
pre_feat_eng = spark.table(f"{catalog_param}.pre_feat_eng").select(
    *id_columns, target_col
)

# COMMAND ----------

from pyspark.sql.functions import udf, col, when, lit, coalesce
from pyspark.sql.types import DoubleType

def handle_null_feature(df, feature_column, fallback_value):
    """
    Handle null values in a feature column using withColumn approach.
    
    :param df: The input DataFrame
    :param feature_column: The name of the column to handle null values
    :param fallback_value: The value to use when the feature is null
    :return: DataFrame with null values handled
    """
    return df.withColumn(
        feature_column,
        coalesce(col(feature_column), lit(fallback_value))
    )

# Example usage:
# df = handle_null_feature(df, "some_feature", 0.0)
  
spark.udf.register("lr_nhl_demo.dev.handle_null_feature", handle_null_feature)

# COMMAND ----------

# TEST

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import lit, col

# Define the schema for the new row
schema = StructType([
    StructField("gameId", StringType(), False),
    StructField("playerId", StringType(), True),
    StructField(target_col, DoubleType(), False)
])

# Create a new DataFrame with the single row
new_row = spark.createDataFrame(
    [("20241010", None, 4.0)],
    schema
)

# Union the new row with the existing DataFrame
pre_feat_eng_updated = pre_feat_eng.union(new_row)

display(pre_feat_eng_updated.filter(col("playerId").isNull()))

# COMMAND ----------

def fill_customer_features(data):
  ''' Returns DataFrame with Nulls Fixed
  '''
  pass

customer_transactions = {"YOUR DATAFRAME"}
new_df = fill_customer_features(customer_transactions)

# UPDATING FEATURE STORE
fe.write_table(
  df=new_df,
  name='{FEATURE STORE NAME}',
  mode='merge'
)

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup, FeatureFunction

fe = FeatureEngineeringClient()

# Define feature lookups
feature_lookups = [
    FeatureLookup(
        table_name=f"{catalog_param}.player_features_{feature_count_param}",
        feature_names=None,  # Include all features
        lookup_key=id_columns,  # The key column used for joining
    ),
    FeatureFunction(
    udf_name="lr_nhl_demo.dev.handle_null_feature",
    input_bindings={
        "df":pre_feat_eng_updated,
        "feature_value": "playerId",
        "fallback_value": '9999-9'
    },
    output_name="playerId"  # This matches the original column name
)
]

training_set = fe.create_training_set(
    df=pre_feat_eng_updated,
    feature_lookups=feature_lookups,
    label=target_col,
    # exclude_columns=id_columns,
)

# training_df = training_set.load_df().toPandas()

# COMMAND ----------

# TEST
display(training_set.load_df().filter(col('playerId') == '9999-9'))

# COMMAND ----------

# Separate target column from features
X = training_df.drop([target_col], axis=1)
y = training_df[target_col]

# COMMAND ----------

import mlflow
from mlflow.models import Model, infer_signature, ModelSignature
from mlflow import pyfunc
from hyperopt import hp, tpe, fmin, STATUS_OK, SparkTrials
import pyspark.pandas as ps
import pandas as pd
from sklearn.pipeline import Pipeline
import lightgbm
from lightgbm import LGBMRegressor
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from hyperopt.pyll.base import scope
from mlflow.pyfunc import PyFuncModel


# Define the objective function to accept different model types
def retrain_objective(params, X_train=pd.DataFrame(), y_train=pd.DataFrame(), full_train_flag=False):

    model_type = params.pop("model_type")

    # Convert numeric parameters to the correct type
    for key, value in params.items():
        if isinstance(value, str):
            try:
                if '.' in value:
                    params[key] = float(value)
                else:
                    params[key] = int(value)
            except ValueError:
                # If conversion fails, leave the parameter as is
                pass

    with mlflow.start_run(experiment_id=training_experiment_param) as mlflow_run:
        mlflow.set_tag("model_type", model_type)
        mlflow.set_tag("features_count", feature_count_param)

        # Select the model based on the model_type parameter
        if model_type == "lightgbm":
            model = LGBMRegressor(**params)
            print(f"Training LightGBM model with params: {params}")
        elif model_type == "linear":
            model = LinearRegression()
            print(f"Training LinearRegression model with params: {params}")
        elif model_type == "random_forest":
            model = RandomForestRegressor(**params)
            print(f"Training RandomForestRegressor model with params: {params}")
        elif model_type == "xgboost":
            model = XGBRegressor(**params)
            print(f"Training XGBRegressor model with params: {params}")
        else:
            raise ValueError(f"Unknown model type: {model_type}")

        # Enable automatic logging of input samples, metrics, parameters, and models
        mlflow.sklearn.autolog(
            log_input_examples=True,
            silent=True,
        )

        # # Log parameters
        # mlflow.log_params(cleaned_params)

        # Create a sample input
        sample_input = X.head(5)  

        # Create a pipeline with the selected model
        pipeline = Pipeline(
            [
                ("regressor", model),
            ]
        )

        pipeline.fit(
            X_train,
            y_train,
        )

        # Infer the model signature
        signature = infer_signature(X, pipeline.predict(X))

        mlflow_model = Model()
        pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
        pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=pipeline)

        fe.log_model(
            model=pyfunc_model,
            artifact_path="model",
            flavor=mlflow.sklearn,
            training_set=training_set,
            registered_model_name=f"{catalog_param}.player_prediction_SOG",
            params=params,
            signature=signature,
            input_example=sample_input,
        )

        loss = "N/A"
        val_metrics = "N/A"
        test_metrics = "N/A"

        return {
            "loss": loss,
            "status": STATUS_OK,
            "val_metrics": val_metrics,
            "test_metrics": test_metrics,
            "model": pipeline,
            "run": mlflow_run,
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Params from best Model for retraining

# COMMAND ----------

def clean_and_prepare_params(best_params, best_model_type):
    def safe_convert(value, to_type):
        if value == 'None' or value is None:
            return None
        try:
            if to_type == int:
                return int(float(value))
            return to_type(value)
        except ValueError:
            return None

    def clean_and_convert_param(key, value):
        if isinstance(value, str):
            value = value.strip().rstrip(',')
        
        if key in ['random_state', 'seed', 'num_iterations', 'n_estimators', 'max_depth', 'min_child_samples', 'num_leaves', 'max_bin', 'subsample_for_bin'] or key.endswith(('_estimators', '_depth', '_samples', '_leaves', '_bin')):
            return safe_convert(value, int)
        elif key in ['learning_rate', 'colsample_bytree', 'subsample', 'lambda_l1', 'lambda_l2', 'min_samples_split', 'min_samples_leaf', 'min_child_weight', 'min_split_gain', 'reg_alpha', 'reg_lambda'] or key.endswith(('_rate', '_bytree', '_l1', '_l2', '_alpha', '_lambda', '_gain')):
            return safe_convert(value, float)
        elif value == 'None' or value is None:
            return None
        elif value.lower() == 'true':
            return True
        elif value.lower() == 'false':
            return False
        else:
            return value

    # Clean all parameters
    cleaned_params = {key: clean_and_convert_param(key, value) for key, value in best_params.items()}

    # Remove redundant 'regressor__' parameters
    cleaned_params = {key.replace('regressor__', ''): value for key, value in cleaned_params.items()}

    # Add verbosity parameter to suppress output
    cleaned_params["verbose"] = -1

    cleaned_params['model_type'] = best_model_type

    # Handle model-specific parameters
    if best_model_type.lower() == 'lightgbm':
        # Ensure 'num_iterations' is present and correctly formatted for LightGBM
        if 'num_iterations' not in cleaned_params:
            cleaned_params['num_iterations'] = cleaned_params.get('n_estimators', 100)
        
        # Remove 'seed' if present (as LightGBM uses 'random_state')
        cleaned_params.pop('seed', None)

    elif best_model_type.lower() == 'xgboost':
        # Ensure 'n_estimators' is present for XGBoost
        if 'n_estimators' not in cleaned_params:
            cleaned_params['n_estimators'] = cleaned_params.get('num_iterations', 100)
        
        # Handle 'missing' parameter for XGBoost
        if 'missing' in cleaned_params:
            if cleaned_params['missing'] == 'nan':
                cleaned_params['missing'] = float('nan')
            elif cleaned_params['missing'] == 'None':
                cleaned_params['missing'] = None
        
        # Remove LightGBM-specific parameters
        for param in ['num_iterations', 'num_leaves', 'min_child_samples']:
            cleaned_params.pop(param, None)

    # Ensure 'random_state' is present and is an integer
    if 'random_state' not in cleaned_params:
        cleaned_params['random_state'] = 729986891
    else:
        cleaned_params['random_state'] = safe_convert(str(cleaned_params['random_state']).replace(',', ''), int)

    # Remove unnecessary parameters
    keys_to_remove = ['regressor', 'steps', 'memory', 'classifier_type']
    for key in keys_to_remove:
        cleaned_params.pop(key, None)

    return cleaned_params

# Usage:

cleaned_params = clean_and_prepare_params(best_params, best_model_type)
print("Final cleaned parameters:")
print(cleaned_params)

# COMMAND ----------

# Retrain on full dataset
result = retrain_objective(cleaned_params, X, y, full_train_flag=True)

print("Retraining completed. Results:")
print(result)

# COMMAND ----------

# Set this flag to True and re-run the notebook to see the SHAP plots
shap_enabled = True

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

from mlflow.tracking import MlflowClient

client = MlflowClient()
runs = client.search_runs(experiment_ids=training_experiment_param, order_by=["created DESC"], max_results=1)
last_run = runs[0]

display(last_run)

# COMMAND ----------

last_run_id = last_run.info.run_id

# COMMAND ----------

# model_uri for the generated model
best_model_uri = f"runs:/{last_run_id}/model"
print(best_model_uri)

# COMMAND ----------

dbutils.jobs.taskValues.set(key="best_model_uri", value=best_model_uri)
print(f"Successfully set the best_model_uri to {best_model_uri}")

# COMMAND ----------

registered_model_version = mlflow.register_model(best_model_uri, f"{catalog_param}.player_prediction_sog")

# COMMAND ----------

# Set as Champion based on max version
client = mlflow.tracking.MlflowClient()
model_version_infos = client.search_model_versions(
    f"name = '{catalog_param}.player_prediction_sog'"
)
new_model_version = max(
    int(model_version_info.version) for model_version_info in model_version_infos
)
client.set_registered_model_alias(
    f"{catalog_param}.player_prediction_sog", "champion", new_model_version
)

print(f"Champion model set to version: {new_model_version}")

# COMMAND ----------

# DBTITLE 1,set tag for feature count
client.set_model_version_tag(
    name=f"{catalog_param}.player_prediction_sog",
    version=str(new_model_version),
    key="features_count",
    value=feature_count_param,
)

print(f"TAG SET: features_count: {feature_count_param}")

# COMMAND ----------


