# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Selection Exploration

# COMMAND ----------

# DBTITLE 1,Create Features Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import (
    SelectFromModel,
    SelectKBest,
    mutual_info_classif,
    RFE,
)
from sklearn.linear_model import LogisticRegression
from databricks.automl_runtime.sklearn.column_selector import ColumnSelector

import lightgbm
from lightgbm import LGBMRegressor

from sklearn import set_config

set_config(transform_output="pandas")

# Create the full pipeline
features_pipeline = Pipeline(
    [
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        (
            "feature_selector",
            SelectFromModel(
                RandomForestRegressor(n_estimators=200, random_state=42),
                max_features=150,
                threshold=-np.inf,
            ),
        ),
    ]
)

# COMMAND ----------

# Now create the FeatureUnion with fitted pipelines
# feature_union = FeatureUnion(
#     [
# ("mi_pipeline", ColumnSelector(columns=mi_pipeline.named_steps['selector'].get_feature_names_out())),
# ("rfe_pipeline", ColumnSelector(columns=rfe_pipeline.named_steps['selector'].get_feature_names_out())),
# ("rf_pipeline", ColumnSelector(columns=SelectFromModel(RandomForestRegressor(n_estimators=5, random_state=42)).get_feature_names_out())),
#     ]
# )

# features_pipeline = Pipeline(
#     [
#         ("column_selector", col_selector),
#         ("preprocessor", preprocessor),
#         ("rf", RandomForestRegressor(n_estimators=5, random_state=42)),
#         ("feature_selector", SelectFromModel(rf, prefit=True)),
#         ("regressor", LGBMRegressor()),
#     ]
# )

# features_pipeline = Pipeline(
#     [
#         ('column_selector', col_selector),
#         ('preprocessor', preprocessor),
#         ('feature_selection_rfe', RFE(estimator=RandomForestClassifier(), n_features_to_select=100, step=10)),
#         ('feature_selection_kbest', SelectKBest(score_func=mutual_info_classif, k=75)),
#         ('feature_selection_rfr', SelectFromModel(RandomForestRegressor(n_estimators=5, random_state=42)))
#     ]
# )

# COMMAND ----------

# DBTITLE 1,Feature Selection Threshold Function
import numpy as np
import matplotlib.pyplot as plt
from sklearn.model_selection import cross_val_score
from sklearn.feature_selection import SelectFromModel
from sklearn.ensemble import RandomForestRegressor
from lightgbm import LGBMRegressor
from sklearn.pipeline import Pipeline


def evaluate_threshold(X_train, y_train, pipeline, thresholds):
    scores = []
    n_features = []

    for threshold in thresholds:
        # Update the feature_selector step with the new threshold
        pipeline.named_steps["feature_selector"] = SelectFromModel(
            RandomForestRegressor(n_estimators=5, random_state=42), threshold=threshold
        )

        # Fit the pipeline to get the selected features
        pipeline.fit(X_train, y_train)

        # Get the number of selected features
        selected_features = pipeline.named_steps["feature_selector"].get_support()
        n_features.append(np.sum(selected_features))

        # Evaluate the pipeline
        score = cross_val_score(
            pipeline, X_train, y_train, cv=5, scoring="neg_mean_squared_error"
        ).mean()
        scores.append(-score)  # Convert to positive MSE

    return scores, n_features


# COMMAND ----------

# DBTITLE 1,Get Optimal Threshold
run_importances = False

if run_importances:
    # Define your pipeline
    features_pipeline = Pipeline(
        [
            ("column_selector", col_selector),
            ("preprocessor", preprocessor),
            (
                "feature_selector",
                SelectFromModel(RandomForestRegressor(n_estimators=5, random_state=42)),
            ),
            ("regressor", LGBMRegressor(random_state=42)),
        ]
    )

    # Define thresholds to try
    thresholds = [1e-5, 1e-4, 1e-3, 1e-2, 0.1, 0.15, 0.2, 0.25]

    # Evaluate the pipeline for different thresholds
    scores, n_features = evaluate_threshold(
        X_train, y_train, features_pipeline, thresholds
    )

    # Plot results
    fig, ax1 = plt.subplots(figsize=(10, 6))

    ax1.set_xlabel("Threshold")
    ax1.set_ylabel("Mean Squared Error", color="tab:blue")
    ax1.plot(thresholds, scores, color="tab:blue", marker="o")
    ax1.tick_params(axis="y", labelcolor="tab:blue")

    ax2 = ax1.twinx()
    ax2.set_ylabel("Number of Features", color="tab:orange")
    ax2.plot(thresholds, n_features, color="tab:orange", marker="s")
    ax2.tick_params(axis="y", labelcolor="tab:orange")

    plt.title("Feature Selection Threshold vs. MSE and Number of Features")
    plt.xscale("log")
    fig.tight_layout()
    plt.show()

    # Print the results
    for threshold, score, n_feat in zip(thresholds, scores, n_features):
        print(f"Threshold: {threshold:.5f}, MSE: {score:.4f}, Features: {n_feat}")

    # Find the threshold with the lowest MSE
    best_threshold = thresholds[np.argmin(scores)]
    print(f"\nBest threshold: {best_threshold:.5f}")

# COMMAND ----------

# DBTITLE 1,feature selection testing
# from sklearn.pipeline import Pipeline
# from sklearn.feature_selection import RFE, RFECV
# from lightgbm import LGBMRegressor
# from sklearn.model_selection import train_test_split, cross_val_score
# from sklearn.datasets import make_regression
# from sklearn.model_selection import RepeatedKFold

# # Initialize the LGBMRegressor
# lgbm = LGBMRegressor(random_state=42)

# # Set up RFECV
# rfecv = RFECV(estimator=lgbm, step=20, cv=RepeatedKFold(n_splits=3, n_repeats=2, random_state=42), scoring='neg_mean_squared_error', min_features_to_select=100, verbose=1)

# # Create a pipeline with RFECV
# pipeline_rfecv = Pipeline([
#     ('column_selector', col_selector),
#     ('preprocessor', preprocessor),
#     ('feature_selection_rfecv', rfecv),
#     ('model', lgbm)
# ])

# COMMAND ----------

# DBTITLE 1,feature selection testing
# set_config(transform_output="pandas")

# # Fit the RFECV pipeline
# pipeline_rfecv.fit(X_train, y_train)

# # Evaluate the RFECV pipeline
# rfecv_score = cross_val_score(pipeline_rfecv, X_train, y_train, cv=5, scoring='neg_mean_squared_error')
# print("RFECV pipeline score:", rfecv_score.mean())

# # Get the number of features selected by RFECV
# print("Optimal number of features selected by RFECV:", rfecv.n_features_)

# COMMAND ----------

# DBTITLE 1,Feature Selection Fit
set_config(transform_output="pandas")

# Fit the pipeline to data
features_pipeline.fit(X_train, y_train)

# COMMAND ----------

# DBTITLE 1,Get minimum importance value
# Get the feature selector step
feature_selector = features_pipeline.named_steps["feature_selector"]

# Get the selected feature importances
importances = feature_selector.estimator_.feature_importances_

# Sort the importances in descending order
sorted_importances = np.sort(importances)[::-1]

# Get the minimum threshold value (importance of the 150th feature)
min_threshold = sorted_importances[149]

print(f"Minimum feature threshold value: {min_threshold}")

# COMMAND ----------

import matplotlib.pyplot as plt

# Slice the sorted importances to get the top 150 features
top_n_importances = sorted_importances[:150]

plt.figure(figsize=(12, 6))
plt.plot(
    range(1, len(top_n_importances) + 1), top_n_importances, marker="o", markersize=3
)
plt.title("Top 150 Feature Importances")
plt.xlabel("Feature Number")
plt.ylabel("Importance")
plt.grid(True)
plt.yscale("log")  # Using log scale to better visualize the drop-off
plt.tight_layout()
plt.show()

# COMMAND ----------

# Assuming 'feature_names' is a list of all feature names
# Get the indices of the top 10 features
feature_names = [f"Feature_{i}" for i in range(len(importances))]
top_10_indices = np.argsort(sorted_importances)[-10:][::-1]

# Get the top 10 feature names and importances
top_10_features = [feature_names[i] for i in top_10_indices]
top_10_importances = sorted_importances[top_10_indices]

# Create a DataFrame
df_top_10 = pd.DataFrame({"Feature": top_10_features, "Importance": top_10_importances})

# Display the DataFrame
print(df_top_10)

# COMMAND ----------

processed_X_train = features_pipeline.transform(X_train)

processed_X_train

# COMMAND ----------

# DBTITLE 1,Get Number of Features Selected
# Get selected columns
selected_columns = features_pipeline.named_steps["feature_selector"].get_support()
print("Selected feature names count:", len(selected_columns))

# TO DO: convert X_train below to be the preprocessed dataset
# feature_name = X_train.columns[selected_columns]
# feature_name


# COMMAND ----------

# MAGIC %md
# MAGIC ### Troubleshooting

# COMMAND ----------

# Get the artifact URI for the saved model
artifact_uri = mlflow.get_artifact_uri("preprocess_model") + "/conda.yaml"

print(f"Model artifact URI: {artifact_uri}")

# The rest of your code remains the same
preprocess_run = mlflow.last_active_run()
run_id = preprocess_run.info.run_id

tmp_dir = tempfile.mkdtemp()
client = mlflow.tracking.MlflowClient()

conda_file_path = mlflow.artifacts.download_artifacts(
    "/Workspace/Users/logan.rupert@databricks.com/NHL_SOG/nhlPredict/src/ML/model_training/preprocess_model/conda.yaml"
)

# COMMAND ----------

preprocess_model_uri = f"runs:/{preprocess_run.info.run_id}/preprocess_model"
preprocess_model_uri

# COMMAND ----------

conda_file_path = mlflow.artifacts.download_artifacts(
    artifact_uri=f"/Workspace/Users/logan.rupert@databricks.com/NHL_SOG/nhlPredict/src/ML/model_training/preprocess_model/conda.yaml",
    dst_path=tmp_dir,
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
        run_id=run_id, local_path=conda_file_path, artifact_path="preprocess_model"
    )

# Fix requirements.txt
venv_file_path = mlflow.artifacts.download_artifacts(
    artifact_uri=f"/Workspace/Users/logan.rupert@databricks.com/NHL_SOG/nhlPredict/src/ML/model_training/preprocess_model/requirements.txt",
    dst_path=tmp_dir,
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
    client.log_artifact(
        run_id=run_id, local_path=venv_file_path, artifact_path="preprocess_model"
    )

shutil.rmtree(tmp_dir)

# Register the model
preprocess_model_uri = f"runs:/{preprocess_run.info.run_id}/preprocess_model"
mlflow.register_model(preprocess_model_uri, "lr_nhl_demo.dev.preprocess_model")

# Set as Champion
client = mlflow.tracking.MlflowClient()
model_version_infos = client.search_model_versions(
    "name = 'lr_nhl_demo.dev.preprocess_model'"
)
new_model_version = max(
    [model_version_info.version for model_version_info in model_version_infos]
)
client.set_registered_model_alias(
    "lr_nhl_demo.dev.preprocess_model", "champion", new_model_version
)

pp_champion_version = client.get_model_version_by_alias(
    "lr_nhl_demo.dev.preprocess_model", "champion"
)

preprocess_model_name = pp_champion_version.name
preprocess_model_version = pp_champion_version.version

preprocess_model_uri = f"models:/{preprocess_model_name}/{preprocess_model_version}"
preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

# COMMAND ----------

import os

model_path = preprocess_model_uri.replace("runs:/", "/dbfs/mlflow/")
print(f"Checking model path: {model_path}")
if os.path.exists(model_path):
    print("Model directory exists")
    print("Contents:")
    print(os.listdir(model_path))
else:
    print("Model directory does not exist")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Previous Methodology

# COMMAND ----------

import mlflow
import os
import shutil
import tempfile
import yaml

preprocess_run = mlflow.last_active_run()
run_id = preprocess_run.info.run_id

# Set up a local dir for downloading the artifacts.
tmp_dir = tempfile.mkdtemp()

client = mlflow.tracking.MlflowClient()

# Fix conda.yaml
conda_file_path = mlflow.artifacts.download_artifacts(
    artifact_uri=f"runs:/{run_id}/preprocess_model/conda.yaml", dst_path=tmp_dir
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
        run_id=run_id, local_path=conda_file_path, artifact_path="preprocess_model"
    )

# Fix requirements.txt
venv_file_path = mlflow.artifacts.download_artifacts(
    artifact_uri=f"runs:/{run_id}/preprocess_model/requirements.txt", dst_path=tmp_dir
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
    client.log_artifact(
        run_id=run_id, local_path=venv_file_path, artifact_path="preprocess_model"
    )

shutil.rmtree(tmp_dir)


# Get the run ID and model URI
preprocess_model_uri = f"runs:/{preprocess_run.info.run_id}/preprocess_model"

# Register the model
mlflow.register_model(preprocess_model_uri, "lr_nhl_demo.dev.preprocess_model")

# Set as Champion based on max version
client = mlflow.tracking.MlflowClient()
model_version_infos = client.search_model_versions(
    "name = 'lr_nhl_demo.dev.preprocess_model'"
)
new_model_version = max(
    [model_version_info.version for model_version_info in model_version_infos]
)
client.set_registered_model_alias(
    "lr_nhl_demo.dev.preprocess_model", "champion", new_model_version
)

pp_champion_version = client.get_model_version_by_alias(
    "lr_nhl_demo.dev.preprocess_model", "champion"
)

preprocess_model_name = pp_champion_version.name
preprocess_model_version = pp_champion_version.version

preprocess_model_uri = f"models:/{preprocess_model_name}/{preprocess_model_version}"
preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

# COMMAND ----------

# Get the run ID and model URI
preprocess_model_uri = f"runs:/{preprocess_run.info.run_id}/preprocess_model"

# Register the model
mlflow.register_model(preprocess_model_uri, "lr_nhl_demo.dev.preprocess_model")

# COMMAND ----------

# Set as Champion based on max version
client = mlflow.tracking.MlflowClient()
model_version_infos = client.search_model_versions(
    "name = 'lr_nhl_demo.dev.preprocess_model'"
)
new_model_version = max(
    [model_version_info.version for model_version_info in model_version_infos]
)
client.set_registered_model_alias(
    "lr_nhl_demo.dev.preprocess_model", "champion", new_model_version
)

pp_champion_version = client.get_model_version_by_alias(
    "lr_nhl_demo.dev.preprocess_model", "champion"
)

preprocess_model_name = pp_champion_version.name
preprocess_model_version = pp_champion_version.version

preprocess_model_uri = f"models:/{preprocess_model_name}/{preprocess_model_version}"
preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

# COMMAND ----------

X_train_processed = preprocess_model.predict(X_train)

# COMMAND ----------

X_train_processed_UC = mlflow.data.load_delta(
    table_name="lr_nhl_demo.dev.X_train_processed", version="0"
)
X_train_processed_UC_PD = X_train_processed_UC.df.toPandas()

X_train_processed_UC_PD
