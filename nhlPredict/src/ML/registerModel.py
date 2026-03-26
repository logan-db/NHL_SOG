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

trial_experiment_param = str(dbutils.widgets.get("trial_experiment_param"))
training_experiment_param = str(dbutils.widgets.get("training_experiment_param"))

target_col = dbutils.widgets.get("target_col")
id_columns = ["gameId", "playerId"]

trial_eval_param = int(dbutils.widgets.get("trial_eval_param"))
catalog_param = dbutils.widgets.get("catalog").lower()

# COMMAND ----------

import mlflow
from mlflow.tracking import MlflowClient
from datetime import datetime, timedelta

def get_best_run(experiment_id, metric_name):
    client = MlflowClient()
    
    # Calculate the timestamp for 24 hours ago
    twenty_four_hours_ago = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)
    
    # Add filter for runs in the last 24 hours
    filter_string = f"attributes.start_time > {twenty_four_hours_ago}"
    
    runs = client.search_runs(
        experiment_id,
        filter_string=filter_string,
        order_by=[f"metrics.{metric_name} DESC"],
        max_results=1
    )
    
    if runs:
        return runs[0]
    else:
        return None  # Return None if no runs are found in the last 24 hours

# COMMAND ----------

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

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

# Define feature lookups
feature_lookups = [
    FeatureLookup(
        table_name=f"{catalog_param}.player_features_{feature_count_param}",
        feature_names=None,  # Include all features
        lookup_key=id_columns,  # The key column used for joining
    )
]

training_set = fe.create_training_set(
    df=pre_feat_eng,
    feature_lookups=feature_lookups,
    label=target_col,
    exclude_columns=id_columns,
)

training_df = training_set.load_df().toPandas()

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

# MAGIC %md
# MAGIC ## Stacked Ensemble (LightGBM + XGBoost + RF + Ridge meta-learner)

# COMMAND ----------

import numpy as np
from sklearn.linear_model import Ridge
from sklearn.model_selection import KFold

def get_best_run_by_type(experiment_id, model_type):
    """Get the best recent run for a specific model type."""
    twenty_four_hours_ago = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)
    runs = client.search_runs(
        experiment_id,
        filter_string=f"attributes.start_time > {twenty_four_hours_ago} AND tags.model_type = '{model_type}'",
        order_by=["metrics.test_r2_score DESC"],
        max_results=1,
    )
    return runs[0] if runs else None


class StackedEnsembleModel(mlflow.pyfunc.PythonModel):
    def __init__(self, base_models, meta_learner):
        self.base_models = base_models
        self.meta_learner = meta_learner

    def predict(self, context, model_input):
        base_preds = np.column_stack([
            m.predict(model_input) for m in self.base_models
        ])
        return self.meta_learner.predict(base_preds)


client = MlflowClient()
model_types = ["lightgbm", "xgboost", "random_forest"]
best_runs_by_type = {}
for mt in model_types:
    run = get_best_run_by_type(trial_experiment_param, mt)
    if run:
        best_runs_by_type[mt] = run
        print(f"Best {mt}: R2={run.data.metrics.get('test_r2_score', 'N/A')}")

if len(best_runs_by_type) >= 2:
    print(f"\nBuilding stacked ensemble from {len(best_runs_by_type)} model types...")

    base_models = []
    for mt, run in best_runs_by_type.items():
        params = clean_and_prepare_params(run.data.params, mt)
        params.pop("model_type")
        params.pop("verbose", None)

        if mt == "lightgbm":
            m = LGBMRegressor(verbose=-1, **params)
        elif mt == "random_forest":
            m = RandomForestRegressor(**params)
        elif mt == "xgboost":
            m = XGBRegressor(**params)
        m.fit(X, y)
        base_models.append(m)
        print(f"  Trained base model: {mt}")

    # Generate out-of-fold predictions for meta-learner training
    n_splits = 5
    kf = KFold(n_splits=n_splits, shuffle=False)
    oof_preds = np.zeros((len(X), len(base_models)))

    for fold_idx, (train_idx, val_idx) in enumerate(kf.split(X)):
        X_fold_train, X_fold_val = X.iloc[train_idx], X.iloc[val_idx]
        y_fold_train = y.iloc[train_idx]

        for model_idx, (mt, run) in enumerate(best_runs_by_type.items()):
            params = clean_and_prepare_params(run.data.params, mt)
            params.pop("model_type")
            params.pop("verbose", None)

            if mt == "lightgbm":
                fold_model = LGBMRegressor(verbose=-1, **params)
            elif mt == "random_forest":
                fold_model = RandomForestRegressor(**params)
            elif mt == "xgboost":
                fold_model = XGBRegressor(**params)
            fold_model.fit(X_fold_train, y_fold_train)
            oof_preds[val_idx, model_idx] = fold_model.predict(X_fold_val)

    meta_learner = Ridge(alpha=1.0)
    meta_learner.fit(oof_preds, y)
    print(f"  Meta-learner weights: {dict(zip(best_runs_by_type.keys(), meta_learner.coef_))}")

    ensemble = StackedEnsembleModel(base_models, meta_learner)

    with mlflow.start_run(experiment_id=training_experiment_param) as ensemble_run:
        mlflow.set_tag("model_type", "stacked_ensemble")
        mlflow.set_tag("features_count", feature_count_param)
        mlflow.set_tag("base_models", ",".join(best_runs_by_type.keys()))

        ensemble_preds = ensemble.predict(None, X)
        from sklearn.metrics import r2_score, mean_absolute_error, mean_squared_error
        mlflow.log_metric("train_r2_score", r2_score(y, ensemble_preds))
        mlflow.log_metric("train_mae", mean_absolute_error(y, ensemble_preds))
        mlflow.log_metric("train_rmse", np.sqrt(mean_squared_error(y, ensemble_preds)))

        signature = infer_signature(X, ensemble_preds)
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=ensemble,
            signature=signature,
            input_example=X.head(5),
            registered_model_name=f"{catalog_param}.player_prediction_SOG",
        )
        print(f"  Stacked ensemble logged: R2={r2_score(y, ensemble_preds):.4f}")
else:
    print(f"Only {len(best_runs_by_type)} model types found; skipping stacking (need >= 2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quantile Regression for Prediction Intervals

# COMMAND ----------

class QuantileRegressionModel(mlflow.pyfunc.PythonModel):
    """Predicts point estimate + quantile intervals (P10, P25, P50, P75, P90)."""

    def __init__(self, point_model, quantile_models):
        self.point_model = point_model
        self.quantile_models = quantile_models

    def predict(self, context, model_input):
        point_pred = self.point_model.predict(model_input)
        result = pd.DataFrame({"predictedSOG": point_pred})
        for q, qmodel in self.quantile_models.items():
            result[f"SOG_p{int(q*100)}"] = qmodel.predict(model_input)
        return result


quantiles = [0.10, 0.25, 0.50, 0.75, 0.90]
quantile_models = {}

for q in quantiles:
    lgb_q = LGBMRegressor(
        objective="quantile",
        alpha=q,
        n_estimators=300,
        max_depth=8,
        learning_rate=0.05,
        num_leaves=63,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=729986891,
        verbose=-1,
    )
    lgb_q.fit(X, y)
    quantile_models[q] = lgb_q
    print(f"Trained quantile model: P{int(q*100)}")

# Use the single-best retrained pipeline as the point model
point_model = result["model"]

quantile_ensemble = QuantileRegressionModel(point_model, quantile_models)

with mlflow.start_run(experiment_id=training_experiment_param) as q_run:
    mlflow.set_tag("model_type", "quantile_ensemble")
    mlflow.set_tag("features_count", feature_count_param)

    q_preds = quantile_ensemble.predict(None, X)
    signature = infer_signature(X, q_preds)
    mlflow.pyfunc.log_model(
        artifact_path="model",
        python_model=quantile_ensemble,
        signature=signature,
        input_example=X.head(5),
        registered_model_name=f"{catalog_param}.player_prediction_SOG_quantile",
    )
    print(f"Quantile ensemble logged with {len(quantiles)} quantiles")

# Set champion alias for quantile model
q_model_name = f"{catalog_param}.player_prediction_SOG_quantile"
q_versions = client.search_model_versions(f"name = '{q_model_name}'")
q_latest = max(int(v.version) for v in q_versions)
client.set_registered_model_alias(q_model_name, "champion", q_latest)
print(f"Quantile model champion set to version {q_latest}")

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


