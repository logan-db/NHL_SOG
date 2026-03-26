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

# DBTITLE 1,time-based split (prevents data leakage for time-series data)
training_df = training_df.sort_values(time_col).reset_index(drop=True)

train_size = int(0.6 * len(training_df))
val_size = int(0.2 * len(training_df))

split_train_df = training_df.iloc[:train_size]
split_val_df = training_df.iloc[train_size : train_size + val_size]
split_test_df = training_df.iloc[train_size + val_size :]

X_train = split_train_df.drop([target_col], axis=1)
y_train = split_train_df[target_col]

X_val = split_val_df.drop([target_col], axis=1)
y_val = split_val_df[target_col]

X_test = split_test_df.drop([target_col], axis=1)
y_test = split_test_df[target_col]

print(f"Time-based split: train={len(X_train)}, val={len(X_val)}, test={len(X_test)}")
print(f"Train date range: {split_train_df[time_col].min()} to {split_train_df[time_col].max()}")
print(f"Val date range:   {split_val_df[time_col].min()} to {split_val_df[time_col].max()}")
print(f"Test date range:  {split_test_df[time_col].min()} to {split_test_df[time_col].max()}")

# Build expanding-window time-series CV folds for more robust hyperparameter evaluation
# Each fold uses all data up to a cutoff for training, and the next chunk for validation
n_cv_folds = 4
total_non_test = len(split_train_df) + len(split_val_df)
fold_size = total_non_test // (n_cv_folds + 1)

X_full_trainval = training_df.iloc[: train_size + val_size].drop([target_col], axis=1)
y_full_trainval = training_df.iloc[: train_size + val_size][target_col]

ts_cv_folds = []
for i in range(n_cv_folds):
    train_end = fold_size * (i + 2)
    val_end = min(train_end + fold_size, total_non_test)
    if train_end >= total_non_test:
        break
    ts_cv_folds.append((list(range(train_end)), list(range(train_end, val_end))))
    print(f"CV Fold {i+1}: train[:{ train_end}], val[{train_end}:{val_end}]")

print(f"Total expanding-window CV folds: {len(ts_cv_folds)}")

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


import numpy as np
from sklearn.metrics import r2_score


def _make_model(model_type, params):
    if model_type == "lightgbm":
        return LGBMRegressor(**params)
    elif model_type == "linear":
        return LinearRegression()
    elif model_type == "random_forest":
        return RandomForestRegressor(**params)
    elif model_type == "xgboost":
        return XGBRegressor(**params)
    raise ValueError(f"Unknown model type: {model_type}")


def objective(params):

    model_type = params.pop("model_type")

    with mlflow.start_run(experiment_id=trial_experiment_param) as mlflow_run:
        mlflow.set_tag("model_type", model_type)
        mlflow.set_tag("features_count", feature_count_param)

        mlflow.sklearn.autolog(
            log_input_examples=True,
            silent=True,
        )

        # Expanding-window time-series CV for robust hyperparameter evaluation
        cv_val_r2_scores = []
        for fold_train_idx, fold_val_idx in ts_cv_folds:
            fold_X_train = X_full_trainval.iloc[fold_train_idx]
            fold_y_train = y_full_trainval.iloc[fold_train_idx]
            fold_X_val = X_full_trainval.iloc[fold_val_idx]
            fold_y_val = y_full_trainval.iloc[fold_val_idx]

            fold_model = _make_model(model_type, params.copy())
            fit_kwargs = {}
            if model_type == "lightgbm":
                fit_kwargs = {
                    "callbacks": [lightgbm.early_stopping(5), lightgbm.log_evaluation(0)],
                    "eval_set": [(fold_X_val, fold_y_val)],
                }
            fold_model.fit(fold_X_train, fold_y_train, **fit_kwargs)
            fold_preds = fold_model.predict(fold_X_val)
            cv_val_r2_scores.append(r2_score(fold_y_val, fold_preds))

        mean_cv_r2 = np.mean(cv_val_r2_scores)
        mlflow.log_metric("mean_cv_r2", mean_cv_r2)
        mlflow.log_metric("std_cv_r2", np.std(cv_val_r2_scores))

        # Final model trained on full train split, evaluated on val and test
        model = _make_model(model_type, params.copy())
        pipeline = Pipeline([("regressor", model)])
        pipeline.fit(
            X_train,
            y_train,
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

        # Use mean CV R2 as the loss to optimize (more robust than single-split)
        loss = -mean_cv_r2

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
        {
            "model_type": "lightgbm",
            "objective": "poisson",
            "colsample_bytree": hp.uniform("lgb_poisson_colsample_bytree", 0.5, 1.0),
            "lambda_l1": hp.loguniform("lgb_poisson_lambda_l1", -5, 0),
            "lambda_l2": hp.loguniform("lgb_poisson_lambda_l2", -5, 2),
            "learning_rate": hp.loguniform("lgb_poisson_learning_rate", -5, -1),
            "max_bin": scope.int(hp.quniform("lgb_poisson_max_bin", 20, 100, 1)),
            "max_depth": scope.int(hp.quniform("lgb_poisson_max_depth", 3, 15, 1)),
            "min_child_samples": scope.int(
                hp.quniform("lgb_poisson_min_child_samples", 20, 200, 1)
            ),
            "n_estimators": scope.int(hp.quniform("lgb_poisson_n_estimators", 100, 500, 1)),
            "num_leaves": scope.int(hp.quniform("lgb_poisson_num_leaves", 31, 255, 1)),
            "subsample": hp.uniform("lgb_poisson_subsample", 0.5, 1.0),
            "random_state": 729986891,
        },
        {
            "model_type": "xgboost",
            "objective": "count:poisson",
            "n_estimators": scope.int(hp.quniform("xgb_poisson_n_estimators", 100, 500, 1)),
            "max_depth": scope.int(hp.quniform("xgb_poisson_max_depth", 3, 15, 1)),
            "learning_rate": hp.loguniform("xgb_poisson_learning_rate", -5, -1),
            "subsample": hp.uniform("xgb_poisson_subsample", 0.5, 1.0),
            "colsample_bytree": hp.uniform("xgb_poisson_colsample_bytree", 0.5, 1.0),
            "random_state": 729986891,
        },
        {
            "model_type": "lightgbm",
            "objective": "tweedie",
            "tweedie_variance_power": hp.uniform("lgb_tweedie_var_power", 1.1, 1.9),
            "colsample_bytree": hp.uniform("lgb_tweedie_colsample_bytree", 0.5, 1.0),
            "lambda_l1": hp.loguniform("lgb_tweedie_lambda_l1", -5, 0),
            "lambda_l2": hp.loguniform("lgb_tweedie_lambda_l2", -5, 2),
            "learning_rate": hp.loguniform("lgb_tweedie_learning_rate", -5, -1),
            "max_depth": scope.int(hp.quniform("lgb_tweedie_max_depth", 3, 15, 1)),
            "min_child_samples": scope.int(
                hp.quniform("lgb_tweedie_min_child_samples", 20, 200, 1)
            ),
            "n_estimators": scope.int(hp.quniform("lgb_tweedie_n_estimators", 100, 500, 1)),
            "num_leaves": scope.int(hp.quniform("lgb_tweedie_num_leaves", 31, 255, 1)),
            "subsample": hp.uniform("lgb_tweedie_subsample", 0.5, 1.0),
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
