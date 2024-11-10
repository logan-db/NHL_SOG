# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Setup

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from pyspark.sql.functions import col
from databricks.feature_engineering import FeatureEngineeringClient

fs = FeatureEngineeringClient()

# COMMAND ----------

# Create widgets with default values
dbutils.widgets.text("n_estimators_param", "100", "Number of estimators")
dbutils.widgets.text("catalog", "lr_nhl_demo.dev", "Catalog name")
dbutils.widgets.text("feature_count", "25", "Number of features")
dbutils.widgets.text("target_col", "player_Total_shotsOnGoal", "target_col")
dbutils.widgets.text("time_col", "gameDate", "time_col")
dbutils.widgets.text("pca_param", "0.95", "pca_param")

# COMMAND ----------

target_col = dbutils.widgets.get("target_col")
time_col = dbutils.widgets.get("time_col")

# Print the parameter values
print(f"time_col: {time_col}")
print(f"target_col: {target_col}")

# COMMAND ----------

# Get Pipeline Params
n_estimators_param = int(dbutils.widgets.get("n_estimators_param"))
catalog_param = dbutils.widgets.get("catalog").lower()
feature_count_param = int(dbutils.widgets.get("feature_count"))
pca_param = float(dbutils.widgets.get("pca_param"))

# Print the parameter values
print(f"n_estimators_param: {n_estimators_param}")
print(f"n_estimators_param: {pca_param}")
print(f"catalog_param: {catalog_param}")
print(f"feature_count_param: {feature_count_param}")

# COMMAND ----------

pre_feat_eng = spark.table(f"{catalog_param}.pre_feat_eng")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select supported columns
# MAGIC Select only the columns that are supported. This allows us to train a model that can predict on a dataset that has extra columns that are not used in training.
# MAGIC `["isPlayoffGame"]` are dropped in the pipelines. See the Alerts tab of the AutoML Experiment page for details on why these columns are dropped.

# COMMAND ----------

cols_to_remove = [
    "DAY",
    "HOME",
    "AWAY",
    "gameId",
    "playerId",
    "shooterName",
    "home_or_away",
    time_col,
]

# Identify numerical and categorical columns
numerical_cols = [
    col
    for col, dtype in pre_feat_eng.dtypes
    if dtype in ["int", "bigint", "float", "double"] and col != target_col
]
categorical_cols = [col for col, dtype in pre_feat_eng.dtypes if dtype == "string"]

numerical_cols = list(set(numerical_cols) - set(cols_to_remove))
categorical_cols = list(set(categorical_cols) - set(cols_to_remove))

# Printing the list of categorical columns
print(categorical_cols)
print(numerical_cols)

# COMMAND ----------

from databricks.automl_runtime.sklearn.column_selector import ColumnSelector

supported_cols = categorical_cols + [col for col in numerical_cols if col != target_col]
# supported_cols = list(set(supported_cols + [time_col]) - set(cols_to_remove))
supported_cols = list(set(supported_cols) - set(cols_to_remove))

col_selector = ColumnSelector(supported_cols)

# COMMAND ----------

target_col in numerical_cols
# time_col in categorical_cols

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Datetime Preprocessor
# MAGIC For each datetime column, extract relevant information from the date:
# MAGIC - Unix timestamp
# MAGIC - whether the date is a weekend
# MAGIC - whether the date is a holiday
# MAGIC
# MAGIC Additionally, extract extra information from columns with timestamps:
# MAGIC - hour of the day (one-hot encoded)
# MAGIC
# MAGIC For cyclic features, plot the values along a unit circle to encode temporal proximity:
# MAGIC - hour of the day
# MAGIC - hours since the beginning of the week
# MAGIC - hours since the beginning of the month
# MAGIC - hours since the beginning of the year

# COMMAND ----------

from sklearn.pipeline import Pipeline

from databricks.automl_runtime.sklearn import DatetimeImputer
from databricks.automl_runtime.sklearn import DateTransformer
from sklearn.preprocessing import StandardScaler

imputers = {
    "gameDate": DatetimeImputer(),
}

date_transformers = []

for col in ["gameDate"]:
    date_preprocessor = Pipeline(
        [
            (f"impute_{col}", imputers[col]),
            (f"transform_{col}", DateTransformer()),
            (f"standardize_{col}", StandardScaler()),
        ]
    )
    date_transformers.append((f"date_{col}", date_preprocessor, [col]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Boolean columns
# MAGIC For each column, impute missing values and then convert into ones and zeros.

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder


bool_imputers = []

bool_pipeline = Pipeline(
    steps=[
        ("cast_type", FunctionTransformer(lambda df: df.astype(object))),
        ("imputers", ColumnTransformer(bool_imputers, remainder="passthrough")),
        (
            "onehot",
            SklearnOneHotEncoder(sparse=False, handle_unknown="ignore", drop="first"),
        ),
    ]
)

bool_transformers = [("boolean", bool_pipeline, ["isHome"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Numerical columns
# MAGIC
# MAGIC Missing values for numerical columns are imputed with mean by default.

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), numerical_cols))

numerical_pipeline = Pipeline(
    steps=[
        (
            "converter",
            FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors="coerce")),
        ),
        ("imputers", ColumnTransformer(num_imputers)),
        ("standardizer", StandardScaler()),
    ]
)

numerical_transformers = [("numerical", numerical_pipeline, numerical_cols)]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Categorical columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Low-cardinality categoricals
# MAGIC Convert each low-cardinality categorical column into multiple binary columns through one-hot encoding.
# MAGIC For each input categorical column (string or numeric), the number of output columns is equal to the number of unique values in the input column.

# COMMAND ----------

print(categorical_cols)

# COMMAND ----------

categorical_cols_value_counts = {}

for col in categorical_cols:
    value_counts = spark.sql(
        f"SELECT COUNT(DISTINCT {col}) as count FROM {catalog_param}.pre_feat_eng"
    ).toPandas()
    categorical_cols_value_counts[col] = value_counts

categorical_cols_value_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ### Index High-Cardinality 'Teams' columns

# COMMAND ----------

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import OrdinalEncoder
from sklearn.pipeline import Pipeline


class CategoricalIndexer(BaseEstimator, TransformerMixin):
    def __init__(self, categorical_cols):
        self.categorical_cols = categorical_cols
        self.encoders = {}

    def fit(self, X, y=None):
        for col in self.categorical_cols:
            encoder = OrdinalEncoder(
                handle_unknown="use_encoded_value", unknown_value=-1
            )
            encoder.fit(X[[col]])
            self.encoders[col] = encoder
        return self

    def transform(self, X):
        X_copy = X.copy()
        for col, encoder in self.encoders.items():
            X_copy[col] = encoder.transform(X_copy[[col]])
        return X_copy


team_categorical_cols = ["playerTeam", "previous_opposingTeam", "opposingTeam"]

indexer_pipeline = Pipeline(
    steps=[
        ("categorical_indexer", CategoricalIndexer(team_categorical_cols)),
    ]
)

indexer_transformers = [("indexer", indexer_pipeline, team_categorical_cols)]

# COMMAND ----------

from databricks.automl_runtime.sklearn import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline, FeatureUnion

one_hot_imputers = []

one_hot_pipeline = Pipeline(
    steps=[
        ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
        ("one_hot_encoder", OneHotEncoder(sparse=False, handle_unknown="indicator")),
    ]
)

categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["position"])]

# COMMAND ----------

from sklearn.compose import ColumnTransformer

transformers = (
    # date_transformers
    bool_transformers
    + numerical_transformers
    + indexer_transformers
    + categorical_one_hot_transformers
)

preprocessor = ColumnTransformer(
    transformers, remainder="passthrough", sparse_threshold=0
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Feature Selection Methods

# COMMAND ----------

from sklearn.feature_selection import SelectKBest, mutual_info_classif

mi_pipeline = Pipeline(
    steps=[
        ("mi_selector", SelectKBest(score_func=mutual_info_classif, k=75)),
    ]
)

# COMMAND ----------

from sklearn.feature_selection import RFE
from sklearn.ensemble import RandomForestClassifier

rfe_pipeline = Pipeline(
    steps=[
        (
            "rfe_selector",
            RFE(estimator=RandomForestClassifier(), n_features_to_select=100, step=10),
        ),
    ]
)

# COMMAND ----------

from sklearn.base import BaseEstimator, TransformerMixin
import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.ensemble import RandomForestRegressor


class FeatureImportanceSelector(BaseEstimator, TransformerMixin):
    def __init__(self, model, n_features):
        self.model = model
        self.n_features = n_features
        self.feature_names_in_ = None

    def fit(self, X, y=None):
        self.feature_names_in_ = X.columns if hasattr(X, "columns") else None
        self.model.fit(X, y)
        self.importances_ = self.model.feature_importances_
        self.indices_ = np.argsort(self.importances_)[-self.n_features :]
        return self

    def transform(self, X):
        if isinstance(X, pd.DataFrame):
            return X.iloc[:, self.indices_]
        return X[:, self.indices_]

    def get_feature_names_out(self):
        if self.feature_names_in_ is not None:
            return np.array(self.feature_names_in_)[self.indices_]
        else:
            return np.array([f"feature_{i}" for i in range(len(self.indices_))])


rf_pipeline = Pipeline(
    steps=[
        (
            "rf_selector",
            FeatureImportanceSelector(RandomForestRegressor(), n_features=200),
        ),
    ]
)

# COMMAND ----------

# feature_union = FeatureUnion(
#     [
#         ("mi_pipeline", mi_pipeline),
#         ("rfe_pipeline", rfe_pipeline),
#         ("rf_pipeline", rf_pipeline),
#     ]
# )

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define Pre-Processing Pipeline
# MAGIC - Dynamic Feature Selection based on 'feature_counts' list
# MAGIC - Creates associated Feature Tables, with only preprocessed and selected features

# COMMAND ----------

# DBTITLE 1,log preprocessing pipeline
import mlflow
import os
import shutil
import tempfile
import yaml
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from xgboost import XGBRegressor
from mlflow.models import infer_signature
from mlflow.pyfunc import PythonModel
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import SelectFromModel
from sklearn.decomposition import PCA
from sklearn import set_config
from sklearn.base import BaseEstimator, TransformerMixin 

class MustHaveDropper(BaseEstimator, TransformerMixin):
    def __init__(self, must_have_features, must_have_features_base):
        self.must_have_features = must_have_features
        self.must_have_features_base = must_have_features_base
        self.selected_features_ = None
        self.must_have_data = None

    def fit(self, X, y=None):
        print("Running Fit function for MustHaveDropper")
        self.selected_features_ = [col for col in self.must_have_features if col in X.columns]

        if not self.selected_features_:
            print("No Valid PreProcessed Must-Have Features Found, Grabbing base Must-Have Features")
            self.selected_features_ = [col for col in self.must_have_features_base if col in X.columns]

        return self

    def transform(self, X):
        if self.selected_features_ is None:
            raise ValueError("MustHaveDropper has not been fitted yet.")
        
        available_features = [feat for feat in self.selected_features_ if feat in X.columns]
        missing_features = set(self.selected_features_) - set(available_features)
        
        if missing_features:
            print(f"Warning: The following must-have features are missing from the DataFrame: {missing_features}")
            print("Available columns:", X.columns.tolist())
        
        if not available_features:
            print("No valid must-have features remaining. Returning the original DataFrame.")
            return X
        
        self.must_have_data = X[available_features]
        print(f"Dropping must-have features from original DataFrame: {len(available_features)}")
        return X.drop(columns=available_features)

    def get_must_have_data(self):
        return self.must_have_data

class MustHaveRejoiner(BaseEstimator, TransformerMixin):
    def __init__(self, must_have_dropper):
        self.must_have_dropper = must_have_dropper

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        must_have_data = self.must_have_dropper.get_must_have_data()
        if must_have_data is not None and not must_have_data.empty:
            print("Concatenating must-have features with original DataFrame")
            # Ensure index alignment before concatenation
            must_have_data.index = X.index
            rejoined_df = pd.concat([X, must_have_data], axis=1)

            print(f"Successful Rejoin of Features: {rejoined_df.columns.tolist()}")

            return rejoined_df
        else:
            print("Warning: No must-have features were stored during transform. Returning the original DataFrame.")
            return X

class PreprocessModel(PythonModel):
    def __init__(self, pipeline, id_columns):
        self.pipeline = pipeline
        self.id_columns = id_columns
        self.feature_names = None

    def fit(self, X, y):
        self.pipeline.fit(X.drop(columns=self.id_columns), y)
        # Store the feature names after fitting
        self.feature_names = self.pipeline.get_feature_names_out()

    def predict(self, context, model_input):
        print("Input columns:", model_input.columns)
        id_data = model_input[self.id_columns]
        
        try:
            transformed_data = self.pipeline.transform(
                model_input.drop(columns=self.id_columns)
            )
            
            # Ensure consistent feature names
            if self.feature_names is not None:
                if isinstance(transformed_data, pd.DataFrame):
                    transformed_data.columns = self.feature_names
                else:
                    transformed_data = pd.DataFrame(transformed_data, columns=self.feature_names)
            
            print("Transformed columns:", transformed_data.columns)
        except Exception as e:
            print("Error during transformation:", str(e))
            raise

        transformed_data.index = id_data.index
        final_data = pd.concat([id_data, transformed_data], axis=1)
        print("Final columns:", final_data.columns)
        return final_data


def create_feature_store_tables(
    X_train, y_train, col_selector, preprocessor, feature_counts, featSelectionModel
):
    """
    Create a single preprocess model and use it to create multiple Feature Store tables for different feature counts.

    This function performs the following steps:
    1. Creates a preprocessing pipeline with a configurable feature selector
    2. Fits the pipeline on the training data
    3. Saves the model using MLflow
    4. Registers the model and sets it as champion
    5. For each feature count:
       a. Configures the feature selector
       b. Transforms the datasets
       c. Creates a Feature Store table with the processed data

    Args:
        X_train (DataFrame): Training features
        y_train (Series): Training target variable
        col_selector: Column selector for the pipeline
        preprocessor: Preprocessor for the pipeline
        feature_counts (list): List of feature counts to iterate over

    Returns:
        None
    """

    set_config(transform_output="pandas")

    
    id_columns = ["gameId", "playerId"]
    must_have_features = [
        "isHome",
        "previous_player_Total_shotsOnGoal",
        "average_player_Total_shotsOnGoal_last_3_games",
        "average_player_Total_shotsOnGoal_last_7_games",
        "previous_perc_rank_rolling_game_Total_goalsFor",
        "previous_perc_rank_rolling_game_Total_shotsOnGoalFor",
        "previous_perc_rank_rolling_game_PP_SOGForPerPenalty",
        "opponent_previous_perc_rank_rolling_game_Total_goalsAgainst",
        "opponent_previous_perc_rank_rolling_game_Total_shotsOnGoalAgainst",
        "opponent_previous_perc_rank_rolling_game_Total_penaltiesFor",
        "opponent_previous_perc_rank_rolling_game_PK_SOGAgainstPerPenalty",
        "matchup_previous_player_Total_shotsOnGoal",
        "matchup_average_player_Total_shotsOnGoal_last_3_games",
        "matchup_average_player_Total_shotsOnGoal_last_7_games"
        ]
    
    must_have_features_renamed = []

    for column in must_have_features:
        new_col = "numerical__impute_mean__" + column
        must_have_features_renamed.append(new_col)

    dropper = MustHaveDropper(must_have_features_renamed, must_have_features)

    # Create a single preprocessing pipeline with a configurable feature selector
    preprocess_pipeline = Pipeline([
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        ('must_have_dropper', dropper),
        ("feature_selector", SelectFromModel(
            featSelectionModel,
            max_features=feature_counts,
            threshold=-np.inf,
        )),
        ("pca", PCA(n_components=0.95, random_state=42)),
        ('must_have_rejoiner', MustHaveRejoiner(dropper))
    ])

    # Fit the pipeline on the full dataset, excluding id columns
    preprocess_pipeline.fit(X_train.drop(columns=id_columns), y_train)

    # Get the PCA step from the pipeline
    pca = preprocess_pipeline.named_steps["pca"]

    # Calculate cumulative explained variance ratio
    cumulative_variance_ratio = np.cumsum(pca.explained_variance_ratio_)

    # Plot the elbow curve
    plt.figure(figsize=(10, 6))
    plt.plot(
        range(1, len(cumulative_variance_ratio) + 1), cumulative_variance_ratio, "bo-"
    )
    plt.xlabel("Number of Components")
    plt.ylabel("Cumulative Explained Variance Ratio")
    plt.title("Elbow Curve for PCA")
    plt.axhline(y=0.95, color="r", linestyle="--", label="95% Explained Variance")
    plt.legend()
    plt.grid(True)

    # Print the number of components used
    print(f"Number of PCA components used: {pca.n_components_}")

    # Get the current working directory
    cwd = os.getcwd()

    # Specify the file name of the model
    file_name = f"preprocess_model_{feature_counts}"

    # Create the full file path by joining the current working directory with the file name
    path = os.path.join(cwd, file_name)

    # Check if the directory exists
    if os.path.exists(path):
        # Check if the path is a directory
        if os.path.isdir(path):
            # Remove the directory and its contents
            shutil.rmtree(path)
            print(f"The directory '{path}' and its contents have been deleted.")
        else:
            print(f"The path '{path}' is not a directory.")
    else:
        print(f"The directory '{path}' does not exist.")

    # Create an instance of the custom PythonModel
    pyfunc_preprocess_model = PreprocessModel(preprocess_pipeline, id_columns)

    # Save the model using MLflow
    with mlflow.start_run(experiment_id="2716391597912916") as run:
        mlflow.pyfunc.save_model(
            path=path,
            python_model=pyfunc_preprocess_model,
            input_example=X_train.iloc[:5],
            signature=infer_signature(X_train, preprocess_pipeline.transform(X_train)),
        )

    print("MODEL RAN!")
    preprocess_model_uri = mlflow.get_artifact_uri(file_name)
    preprocess_run = mlflow.last_active_run()
    run_id = preprocess_run.info.run_id

    print(f"Model preprocess_model_uri: {preprocess_model_uri}")
    print(f"preprocess_run: {preprocess_run}")
    print(f"Model run_id: {run_id}")

    artifact_uri_base_path = path

    # Set up a local dir for downloading the artifacts.
    tmp_dir = tempfile.mkdtemp()

    client = mlflow.tracking.MlflowClient()

    # Download and log PCA Plot
    plt.savefig(f"{artifact_uri_base_path}/pcaPlot.png")
    plt.show()
    
    client.log_artifact(
        run_id=run_id, local_path=f"{artifact_uri_base_path}/pcaPlot.png", artifact_path=file_name
    )
    plt.close()

    print("DOWNLOAD START - CONDA")
    # Fix conda.yaml
    conda_file_path = mlflow.artifacts.download_artifacts(
        artifact_uri=f"{artifact_uri_base_path}/conda.yaml", dst_path=tmp_dir
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
            run_id=run_id, local_path=conda_file_path, artifact_path=file_name
        )

    print("DOWNLOAD START - REQS")
    # Fix requirements.txt
    venv_file_path = mlflow.artifacts.download_artifacts(
        artifact_uri=f"{artifact_uri_base_path}/requirements.txt", dst_path=tmp_dir
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
            run_id=run_id, local_path=venv_file_path, artifact_path=file_name
        )

    shutil.rmtree(tmp_dir)

    uc_model_name = f"{catalog_param}.{file_name}"

    # Register the model
    print(f"Registering model {uc_model_name}...")
    mlflow.register_model(artifact_uri_base_path, uc_model_name)

    # Set as Champion
    print("Setting model as Champion...")

    model_version_infos = client.search_model_versions(f"name = '{uc_model_name}'")
    new_model_version = max(
        [int(model_version_info.version) for model_version_info in model_version_infos]
    )
    client.set_registered_model_alias(uc_model_name, "champion", new_model_version)

    print(f"Model set as Champion for {uc_model_name} version {new_model_version}...")

    pp_champion_version = client.get_model_version_by_alias(uc_model_name, "champion")

    assert (
        int(pp_champion_version.version) == new_model_version
    ), "Preprocess Model version mismatch"

    preprocess_model_name = pp_champion_version.name
    preprocess_model_version = pp_champion_version.version

    preprocess_model_uri = f"models:/{preprocess_model_name}/{preprocess_model_version}"
    print(f"preprocess_model_uri: {preprocess_model_uri}")
    preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

    mlflow.pyfunc.get_model_dependencies(preprocess_model_uri)

    print(f"Creating Feature Store table for {feature_counts} features...")
    
    # Transform datasets using the configured model
    X_train_processed = pyfunc_preprocess_model.predict(None, X_train)

    X_train_processed_spark = spark.createDataFrame(X_train_processed)

    display(X_train_processed_spark)

    table_name = f"{catalog_param}.player_features_{feature_counts}"

    print(f"Creating Feature Store table {table_name}...")

    try:
        fs.drop_table(table_name)
        print(f"Dropped Feature Store table {table_name}...")
    except:
        pass

    fs.create_table(
        name=table_name,
        primary_keys=id_columns,
        schema=X_train_processed_spark.schema,
        description=f"Pre-Processed data with feature selection applied: {feature_counts} Features included. Represents player stats, game by game to be used for player level predictions",
    )

    fs.write_table(
        name=table_name,
        df=X_train_processed_spark,
        # mode="overwrite",
    )

    print(f"Feature Store table {table_name} created SUCCESSFULLY...")

# COMMAND ----------

# Convert pre_feat_eng to Pandas DataFrame
pre_feat_eng_pd = pre_feat_eng.toPandas()

# Separate target column from features
X = pre_feat_eng_pd.drop([target_col], axis=1)
y = pre_feat_eng_pd[target_col]

# COMMAND ----------

featSelectionModel = XGBRegressor(
    n_estimators=n_estimators_param,
    learning_rate=0.1,
    max_depth=5,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42
)
# featSelectionModel = RandomForestRegressor(n_estimators=n_estimators_param, random_state=42)

print(f"Feature Engineering Pipeline RUNNING on {feature_count_param} features")
create_feature_store_tables(
    X, y, col_selector, preprocessor, feature_count_param, featSelectionModel
)
print(f"Feature Engineering Pipeline COMPLETE on {feature_count_param} features")

# COMMAND ----------

mlflow.end_run()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TESTING

# COMMAND ----------

testing = True

if testing:
    dbutils.notebook.exit("success")

# COMMAND ----------

set_config(transform_output="pandas")

# mlflow.end_run()
id_columns = ["gameId", "playerId"]

client = mlflow.tracking.MlflowClient()

feature_counts = 25

uc_model_name = f"{catalog_param}.preprocess_model_{feature_counts}"

# Set as Champion
print("Setting model as Champion...")

model_version_infos = client.search_model_versions(f"name = '{uc_model_name}'")
new_model_version = max(
    [int(model_version_info.version) for model_version_info in model_version_infos]
)
client.set_registered_model_alias(uc_model_name, "champion", new_model_version)

print(f"Model set as Champion for {uc_model_name} version {new_model_version}...")

pp_champion_version = client.get_model_version_by_alias(uc_model_name, "champion")

assert (
    int(pp_champion_version.version) == new_model_version
), "Preprocess Model version mismatch"

preprocess_model_name = pp_champion_version.name
preprocess_model_version = pp_champion_version.version

preprocess_model_uri = f"models:/{preprocess_model_name}/{preprocess_model_version}"
preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

mlflow.pyfunc.get_model_dependencies(preprocess_model_uri)

# COMMAND ----------

model_version_infos = client.search_model_versions(f"name = '{uc_model_name}'")
new_model_version = max(
    [int(model_version_info.version) for model_version_info in model_version_infos]
)

new_model_version

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS lr_nhl_demo.dev.player_features_100;

# COMMAND ----------

print(f"Creating Feature Store table for {feature_counts} features...")

X_train_processed = preprocess_model.predict(X)

X_train_processed_spark = spark.createDataFrame(X_train_processed)

display(X_train_processed_spark)

table_name = f"{catalog_param}.player_features_{feature_counts}"

# COMMAND ----------

print(f"Creating Feature Store table {table_name}...")

try:
    fs.drop_table(table_name)
    print(f"Dropped Feature Store table {table_name}...")
except:
    pass

fs.create_table(
    name=table_name,
    primary_keys=id_columns,
    schema=X_train_processed_spark.schema,
    description=f"Pre-Processed data with feature selection applied: {feature_counts} Features included. Represents player stats, game by game to be used for player level predictions",
)

fs.write_table(
    name=table_name,
    df=X_train_processed_spark,
)

print(f"Feature Store table {table_name} created SUCCESSFULLY...")

# COMMAND ----------

df = fs.read_table(name=table_name)
display(df)

# COMMAND ----------

# DBTITLE 1,Indexing
model_input = pre_feat_eng.toPandas()

id_columns = ["gameId", "playerId"]

id_data = model_input[id_columns]
transformed_data = model_input.drop(columns=id_columns)

id_data

# COMMAND ----------

# Ensure index alignment before concatenation
transformed_data.index = id_data.index

# COMMAND ----------

import pandas as pd

# Concatenate id_data with transformed_data
final_data = pd.concat([id_data, transformed_data], axis=1)
final_data

# COMMAND ----------

display(pre_feat_eng.filter(col("playerId") == 8470604))
display(pre_feat_eng.filter(col("playerId") == 8484255))

assert (
    pre_feat_eng.filter(col("playerId") == 8479318)
    .select("shooterName")
    .collect()[0][0]
    == "Auston Matthews"
)

# COMMAND ----------

display(
    pre_feat_eng.filter((col("playerId") == 8479318) & (col("gameId") == 2023020026))
)  # should be Auston Matthews, TOR vs MIN
