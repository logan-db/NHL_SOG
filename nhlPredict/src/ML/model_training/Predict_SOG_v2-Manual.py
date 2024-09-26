# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Data and Create Training Set

# COMMAND ----------

from sklearn.model_selection import train_test_split

target_col = dbutils.widgets.get("target_col")
time_col = dbutils.widgets.get("time_col")
id_columns = ["gameId", "playerId"]
trial_eval_param = int(dbutils.widgets.get("trial_eval_param"))
catalog_param = dbutils.widgets.get("catalog").lower()
feature_count_param = int(dbutils.widgets.get("feature_count"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Label Columns to Feature Table for Training Purposes

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

# MAGIC %md
# MAGIC ## Train - Validation - Test Split
# MAGIC The input data is split by AutoML into 3 sets:
# MAGIC - Train (60% of the dataset used to train the model)
# MAGIC - Validation (20% of the dataset used to tune the hyperparameters of the model)
# MAGIC - Test (20% of the dataset used to report the true performance of the model on an unseen dataset)
# MAGIC
# MAGIC `_automl_split_col_0000` contains the information of which set a given row belongs to.
# MAGIC We use this column to split the dataset into the above 3 sets.
# MAGIC The column should not be used for training so it is dropped after split is done.
# MAGIC
# MAGIC Given that `gameDate` is provided as the `time_col`, the data is split based on time order,
# MAGIC where the most recent data is split to the test data.

# COMMAND ----------

# DBTITLE 1,random split
# Separate target column from features
X = training_df.drop([target_col], axis=1)
y = training_df[target_col]

# Split the data into train and test datasets
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Split the train dataset into train and validation datasets
X_train, X_val, y_train, y_val = train_test_split(
    X_train, y_train, test_size=0.25, random_state=42
)

# COMMAND ----------

# DBTITLE 1,time col split
# # Convert df_loaded to Pandas DataFrame
# df_loaded_pd = df_loaded.toPandas()

# # Sort the DataFrame based on the date column
# df_loaded_pd = df_loaded_pd.sort_values(time_col)

# # Determine the indices to split the DataFrame
# train_size = int(0.6 * len(df_loaded_pd))
# val_size = int(0.2 * len(df_loaded_pd))

# train_indices = list(range(train_size))
# val_indices = list(range(train_size, train_size + val_size))
# test_indices = list(range(train_size + val_size, len(df_loaded_pd)))

# # Split the DataFrame into training, validation, and test sets
# split_train_df = df_loaded_pd.iloc[train_indices]
# split_val_df = df_loaded_pd.iloc[val_indices]
# split_test_df = df_loaded_pd.iloc[test_indices]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train regression model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under [this MLflow experiment](#mlflow/experiments/2824690123542843)
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment
# MAGIC - To view the full list of tunable hyperparameters, check the output of the cell below

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the objective function
# MAGIC The objective function used to find optimal hyperparameters. By default, this notebook only runs
# MAGIC this function once (`max_evals=1` in the `hyperopt.fmin` invocation) with fixed hyperparameters, but
# MAGIC hyperparameters can be tuned by modifying `space`, defined below. `hyperopt.fmin` will then use this
# MAGIC function's return value to search the space to minimize the loss.

# COMMAND ----------

import mlflow
from mlflow.models import Model, infer_signature, ModelSignature
from mlflow import pyfunc
from hyperopt import hp, tpe, fmin, STATUS_OK, SparkTrials
import pyspark.pandas as ps
from sklearn.pipeline import Pipeline
import lightgbm
from lightgbm import LGBMRegressor
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from hyperopt.pyll.base import scope
from mlflow.pyfunc import PyFuncModel


# Define the objective function to accept different model types
def objective(params, X=[], y=[], full_train_flag=False):

    model_type = params.pop("model_type")

    with mlflow.start_run(experiment_id="634720160613016") as mlflow_run:
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

        # Create a pipeline with the selected model
        pipeline = Pipeline(
            [
                ("regressor", model),
            ]
        )

        if full_train_flag:
            X_train = X
            y_train = y

            pipeline.fit(
                X_train,
                y_train,
            )

            mlflow_model = Model()
            pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
            pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=pipeline)

            fe.log_model(
                model=pyfunc_model,
                artifact_path="player_prediction_SOG",
                flavor=mlflow.sklearn,
                training_set=training_set,
                registered_model_name=f"{catalog_param}.player_prediction_SOG",
                params=params,
            )
            loss = "N/A"
            val_metrics = "N/A"
            test_metrics = "N/A"

        else:

            pipeline.fit(
                X_train,
                y_train,
                # These callbacks are specific to LightGBM, so they should only be used for LightGBM
                **(
                    {
                        "regressor__callbacks": [
                            lightgbm.early_stopping(5),
                            lightgbm.log_evaluation(0),
                        ],
                        "regressor__eval_set": [(X_val, y_val)],
                    }
                    if model_type == "lightgbm"
                    else {}
                ),
            )

            # Log metrics for the training set
            mlflow_model = Model()
            pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
            pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=pipeline)
            training_eval_result = mlflow.evaluate(
                model=pyfunc_model,
                data=X_train.assign(**{str(target_col): y_train}),
                targets=target_col,
                model_type="regressor",
                evaluator_config={
                    "log_model_explainability": False,
                    "metric_prefix": "training_",
                },
            )

            # Log metrics for the validation set
            val_eval_result = mlflow.evaluate(
                model=pyfunc_model,
                data=X_val.assign(**{str(target_col): y_val}),
                targets=target_col,
                model_type="regressor",
                evaluator_config={
                    "log_model_explainability": False,
                    "metric_prefix": "val_",
                },
            )
            val_metrics = val_eval_result.metrics

            # Log metrics for the test set
            test_eval_result = mlflow.evaluate(
                model=pyfunc_model,
                data=X_test.assign(**{str(target_col): y_test}),
                targets=target_col,
                model_type="regressor",
                evaluator_config={
                    "log_model_explainability": False,
                    "metric_prefix": "test_",
                },
            )
            test_metrics = test_eval_result.metrics

            loss = -val_metrics["val_r2_score"]

            # Truncate metric key names so they can be displayed together
            val_metrics = {k.replace("val_", ""): v for k, v in val_metrics.items()}
            test_metrics = {k.replace("test_", ""): v for k, v in test_metrics.items()}

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
# MAGIC ### Configure the hyperparameter search space
# MAGIC Configure the search space of parameters. Parameters below are all constant expressions but can be
# MAGIC modified to widen the search space. For example, when training a decision tree regressor, to allow
# MAGIC the maximum tree depth to be either 2 or 3, set the key of 'max_depth' to
# MAGIC `hp.choice('max_depth', [2, 3])`. Be sure to also increase `max_evals` in the `fmin` call below.
# MAGIC
# MAGIC See https://docs.databricks.com/applications/machine-learning/automl-hyperparam-tuning/index.html
# MAGIC for more information on hyperparameter tuning as well as
# MAGIC http://hyperopt.github.io/hyperopt/getting-started/search_spaces/ for documentation on supported
# MAGIC search expressions.
# MAGIC
# MAGIC For documentation on parameters used by the model in use, please see:
# MAGIC https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.LGBMRegressor.html
# MAGIC
# MAGIC NOTE: The above URL points to a stable version of the documentation corresponding to the last
# MAGIC released version of the package. The documentation may differ slightly for the package version
# MAGIC used by this notebook.

# COMMAND ----------

from hyperopt.pyll.base import scope

# Define the search space including the model type
space = hp.choice(
    "classifier_type",
    [
        {
            "model_type": "lightgbm",
            "colsample_bytree": hp.uniform("colsample_bytree", 0.5, 1.0),
            "lambda_l1": hp.loguniform("lambda_l1", -5, 0),
            "lambda_l2": hp.loguniform("lambda_l2", -5, 2),
            "learning_rate": hp.loguniform("learning_rate", -5, -1),
            "max_bin": scope.int(hp.quniform("max_bin", 20, 100, 1)),
            "max_depth": scope.int(hp.quniform("max_depth", 3, 15, 1)),
            "min_child_samples": scope.int(
                hp.quniform("min_child_samples", 20, 200, 1)
            ),
            "n_estimators": scope.int(hp.quniform("n_estimators", 100, 500, 1)),
            "num_leaves": scope.int(hp.quniform("num_leaves", 31, 255, 1)),
            "subsample": hp.uniform("subsample", 0.5, 1.0),
            "random_state": 729986891,
        },
        {
            "model_type": "random_forest",
            "n_estimators": scope.int(hp.quniform("rf_n_estimators", 100, 500, 1)),
            "max_depth": scope.int(hp.quniform("rf_max_depth", 3, 15, 1)),
            "min_samples_split": hp.uniform("rf_min_samples_split", 0.1, 1.0),
            "min_samples_leaf": hp.uniform("rf_min_samples_leaf", 0.1, 0.5),
            "random_state": 729986891,
        },
        {
            "model_type": "xgboost",
            "n_estimators": scope.int(hp.quniform("xgb_n_estimators", 100, 500, 1)),
            "max_depth": scope.int(hp.quniform("xgb_max_depth", 3, 15, 1)),
            "learning_rate": hp.loguniform("xgb_learning_rate", -5, -1),
            "subsample": hp.uniform("xgb_subsample", 0.5, 1.0),
            "colsample_bytree": hp.uniform("xgb_colsample_bytree", 0.5, 1.0),
            "random_state": 729986891,
        },
    ],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run trials
# MAGIC When widening the search space and training multiple models, switch to `SparkTrials` to parallelize
# MAGIC training on Spark:
# MAGIC ```
# MAGIC from hyperopt import SparkTrials
# MAGIC trials = SparkTrials()
# MAGIC ```
# MAGIC
# MAGIC NOTE: While `Trials` starts an MLFlow run for each set of hyperparameters, `SparkTrials` only starts
# MAGIC one top-level run; it will start a subrun for each set of hyperparameters.
# MAGIC
# MAGIC See http://hyperopt.github.io/hyperopt/scaleout/spark/ for more info.

# COMMAND ----------

import pandas as pd
from sklearn import set_config

trials = SparkTrials()

# Run the optimization
fmin(
    fn=objective,
    space=space,
    algo=tpe.suggest,
    max_evals=trial_eval_param,
    trials=trials,
)

best_result = trials.best_trial["result"]
model = best_result["model"]
mlflow_run = best_result["run"]

display(
    pd.DataFrame(
        [best_result["val_metrics"], best_result["test_metrics"]],
        index=["validation", "test"],
    )
)

set_config(display="diagram")
model

# COMMAND ----------

# Assuming you have the best_run_id from the previous experiment
best_run_id = mlflow_run.info.run_id

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
