# Databricks notebook source
import mlflow
from pyspark.sql.functions import col
from databricks.feature_engineering import FeatureEngineeringClient

fs = FeatureEngineeringClient()

# COMMAND ----------

# Create widgets with default values
dbutils.widgets.text("catalog", "lr_nhl_demo.dev", "Catalog name")
dbutils.widgets.text("feature_count", "25", "Number of features")
dbutils.widgets.text("target_col", "player_Total_shotsOnGoal", "target_col")
dbutils.widgets.text("time_col", "gameDate", "time_col")

# COMMAND ----------

target_col = dbutils.widgets.get("target_col")
time_col = dbutils.widgets.get("time_col")
id_columns = ["gameId", "playerId"]

# Print the parameter values
print(f"time_col: {time_col}")
print(f"target_col: {target_col}")

# COMMAND ----------

# Get Pipeline Params
catalog_param = dbutils.widgets.get("catalog").lower()
feature_count_param = int(dbutils.widgets.get("feature_count"))

# Print the parameter values
print(f"catalog_param: {catalog_param}")
print(f"feature_count_param: {feature_count_param}")

# COMMAND ----------

pre_feat_eng = spark.table(f"{catalog_param}.pre_feat_eng")

# COMMAND ----------

file_name = f"preprocess_model_{feature_count_param}"

uc_model_name = f"{catalog_param}.{file_name}"
print(uc_model_name)

# COMMAND ----------

client = mlflow.tracking.MlflowClient()

champion_version_pp = client.get_model_version_by_alias(
    f"{catalog_param}.preprocess_model_{feature_count_param}", "champion"
)

preprocess_model_name = champion_version_pp.name
preprocess_model_version = champion_version_pp.version

preprocess_model_uri = f"models:/{preprocess_model_name}/{preprocess_model_version}"
mlflow.pyfunc.get_model_dependencies(preprocess_model_uri)
preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

print(f"Using Preprocess Model Version: {preprocess_model_version}")

# COMMAND ----------

from sklearn import set_config

def create_feature_store_table(preprocess_model, X_train, feature_counts, catalog_param, id_columns):
    print(f"Creating Feature Store table for {feature_counts} features...")

    set_config(transform_output="pandas")

    # Transform datasets using the configured model
    X_train_processed = preprocess_model.predict(X_train)

    X_train_processed_spark = spark.createDataFrame(X_train_processed)

    display(X_train_processed_spark)

    table_name = f"{catalog_param}.player_features_{feature_counts}"

    print(f"Creating Feature Store table {table_name}...")

    fs.create_table(
        name=table_name,
        primary_keys=id_columns,
        schema=X_train_processed_spark.schema,
        description=f"Pre-Processed data with feature selection applied: {feature_counts} Features included. Represents player stats, game by game to be used for player level predictions",
    )

    print("MERGING INTO EXISTING TABLE")

    fs.write_table(
        name=table_name,
        df=X_train_processed_spark,
        mode="merge",
    )

    print(f"Feature Store table {table_name} created SUCCESSFULLY...")

# COMMAND ----------

# Convert pre_feat_eng to Pandas DataFrame
pre_feat_eng_pd = pre_feat_eng.toPandas()

# Separate target column from features
X = pre_feat_eng_pd.drop([target_col], axis=1)
y = pre_feat_eng_pd[target_col]

# COMMAND ----------

print(f"Feature Engineering Pipeline RUNNING on {feature_count_param} features")
create_feature_store_table(
    preprocess_model=preprocess_model, X_train=X, feature_counts=feature_count_param, catalog_param=catalog_param, id_columns=id_columns
)
print(f"Feature Engineering Pipeline COMPLETE on {feature_count_param} features")

# COMMAND ----------


