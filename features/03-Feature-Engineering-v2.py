# Databricks notebook source
# DBTITLE 1,Load model data
gold_model_stats = spark.table("lr_nhl_demo.dev.gold_model_stats_delta_v2")

# COMMAND ----------

# DBTITLE 1,Imports and Int/Str columns
from pyspark.sql.functions import col
from pyspark.sql.types import StringType
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
import mlflow
import databricks.automl_runtime

# Identify numerical and categorical columns
numerical_cols = gold_model_stats.select_dtypes(include=['int64', 'float64']).columns
# categorical_cols = df.select_dtypes(include=['object']).columns

# Assuming `gold_model_stats` is your DataFrame
categorical_cols = [f.name for f in gold_model_stats.schema.fields if isinstance(f.dataType, StringType)]

# Printing the list of categorical columns
print(categorical_cols)

# COMMAND ----------

import mlflow
import databricks.automl_runtime

target_col = "player_ShotsOnGoalInGame"
time_col = "gameDate"

# COMMAND ----------

# Preprocessing for numerical data
numerical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])

# Preprocessing for categorical data
categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='constant', fill_value='missing')),
    ('onehot', OneHotEncoder(handle_unknown='ignore'))
])

# Combine preprocessing steps
preprocessor = ColumnTransformer(
    transformers=[
        ('num', numerical_transformer, numerical_cols),
        ('cat', categorical_transformer, categorical_cols)
    ])

# Apply preprocessing
X = gold_model_stats.drop('target', axis=1)
y = gold_model_stats['target']
X_preprocessed = preprocessor.fit_transform(X)

# COMMAND ----------

from pyspark.sql import functions as F

min_player_matchup_played_rolling = gold_model_stats.agg(F.min("playerGamesPlayedRolling")).collect()[0][0]
min_player_matchup_played_rolling

# COMMAND ----------

display(
  gold_model_stats.filter(col("playerMatchupPlayedRolling")==0)
)

# COMMAND ----------

display(
  gold_model_stats.filter(col("gameId").isNull())
)

# COMMAND ----------

model_remove_1st_and_upcoming_games = (
  gold_model_stats.filter((col("gameId").isNotNull()) & (col("playerGamesPlayedRolling") > 0) & (col("rolling_playerTotalTimeOnIceInGame") > 180))
)

model_remove_1st_and_upcoming_games.count()

# COMMAND ----------

upcoming_games = (
  gold_model_stats.filter((col("gameId").isNull())
                           & (col("playerGamesPlayedRolling") > 0) 
                           & (col("rolling_playerTotalTimeOnIceInGame") > 180)
                           & (col("gameDate") != "2024-01-17")
                           )
)

display(upcoming_games)

# COMMAND ----------

assert model_remove_1st_and_upcoming_games.count() == model_remove_1st_and_upcoming_games.select('gameId', 'playerId').distinct().count()

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient 

# customer_features_df = compute_customer_features(df) 

fs = FeatureStoreClient() 

try:
  #drop table if exists
  fs.drop_table(f'lr_nhl_demo.dev.SOG_features_v2')
except:
  pass

customer_feature_table = fs.create_table( 
    name='lr_nhl_demo.dev.SOG_features_v2', 
    primary_keys=['gameId', 'playerId'],
    schema=model_remove_1st_and_upcoming_games.schema, 
    description='Skater game by game features' 
)

fs.write_table( 
    name='lr_nhl_demo.dev.SOG_features_v2', 
    df = model_remove_1st_and_upcoming_games, 
    mode = 'overwrite' 
)

# COMMAND ----------


