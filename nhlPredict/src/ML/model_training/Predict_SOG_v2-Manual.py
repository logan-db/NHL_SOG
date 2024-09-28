# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Data and Create Training Set

# COMMAND ----------

# Create widgets with default values
dbutils.widgets.text("catalog", "lr_nhl_demo.dev", "Catalog name")
dbutils.widgets.text("feature_count", "100", "Number of features")
dbutils.widgets.text("target_col", "player_Total_shotsOnGoal", "target_col")
dbutils.widgets.text("time_col", "gameDate", "time_col")
dbutils.widgets.text("trial_eval_param", "10", "trial_eval_param")
dbutils.widgets.text(
    "trial_experiment_param", "4320825364109465", "trial_experiment_param"
)

# COMMAND ----------

from sklearn.model_selection import train_test_split

target_col = dbutils.widgets.get("target_col")
time_col = dbutils.widgets.get("time_col")
id_columns = ["gameId", "playerId"]

trial_eval_param = int(dbutils.widgets.get("trial_eval_param"))
catalog_param = dbutils.widgets.get("catalog").lower()
feature_count_param = int(dbutils.widgets.get("feature_count"))
trial_experiment_param = str(dbutils.widgets.get("trial_experiment_param"))

# COMMAND ----------

# Display the values of the variables
print(f"target_col: {target_col}")
print(f"time_col: {time_col}")
print(f"id_columns: {id_columns}")
print(f"trial_eval_param: {trial_eval_param}")
print(f"catalog_param: {catalog_param}")
print(f"feature_count_param: {feature_count_param}")
print(f"trial_experiment_param: {trial_experiment_param}")

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
def objective(params):

    model_type = params.pop("model_type")

    with mlflow.start_run(experiment_id=trial_experiment_param) as mlflow_run:
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

from hyperopt import hp
from hyperopt.pyll.base import scope

# Define the search space including the model type and the new arguments
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

mlflow.set_experiment(experiment_id=trial_experiment_param)

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

# model_uri for the generated model
best_model_uri = f"runs:/{ mlflow_run.info.run_id }/model"
print(best_model_uri)

# dbutils.jobs.taskValues.set(key="best_model_uri", value=best_model_uri)
# print(f"Successfully set the best_model_uri to {best_model_uri}")

# COMMAND ----------
