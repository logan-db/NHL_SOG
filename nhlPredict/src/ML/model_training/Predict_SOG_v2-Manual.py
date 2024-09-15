# Databricks notebook source
import mlflow

target_col = "player_Total_shotsOnGoal"
time_col = "gameDate"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------

import shutil
import pandas as pd

df_loaded = spark.table("lr_nhl_demo.dev.SOG_features_v2")

# Preview data
display(df_loaded.limit(5))

# COMMAND ----------

# import mlflow
# import os
# import uuid
# import shutil
# import pandas as pd

# # Create temp directory to download input data from MLflow
# input_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
# os.makedirs(input_temp_dir)


# # Download the artifact and read it into a pandas DataFrame
# input_data_path = mlflow.artifacts.download_artifacts(run_id="1913013715404328826999b3d06dc834", artifact_path="data", dst_path=input_temp_dir)

# df_loaded = pd.read_parquet(os.path.join(input_data_path, "training_data"))
# # Delete the temp data
# shutil.rmtree(input_temp_dir)

# # Preview data
# display(df_loaded.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select supported columns
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
    for col, dtype in df_loaded.dtypes
    if dtype in ["int", "bigint", "float", "double"] and col != target_col
]
categorical_cols = [col for col, dtype in df_loaded.dtypes if dtype == "string"]

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

# from databricks.automl_runtime.sklearn.column_selector import ColumnSelector
# supported_cols = ["matchup_average_game_hitsFor_last_3_games", "average_game_blockedShotAttemptsAgainst_last_3_games", "previous_player_PenaltyKillShotsInGame", "gameId", "previous_game_takeawaysFor", "matchup_previous_player_ShotsOnRushesInGame", "matchup_average_game_giveawaysFor_last_7_games", "previous_game_blockedShotAttemptsAgainst", "matchup_average_player_PowerPlayShotAttemptsInGame_last_7_games", "average_game_goalPercentageAgainst_last_3_games", "previous_player_ShotsOnRushesInGame", "matchup_previous_game_takeawaysAgainst", "matchup_average_game_shotsOnGoalFor_last_3_games", "teamMatchupPlayedRolling", "average_game_giveawaysFor_last_7_games", "matchup_previous_game_shotsOnGoalAgainst", "average_game_fenwickPercentage_last_3_games", "previous_game_goalPercentageFor", "previous_player_EvenStrengthShotsInGame", "previous_game_hitsAgainst", "matchup_average_player_PowerPlayShotsInGame_last_7_games", "matchup_average_player_ShotsOnRushesInGame_last_3_games", "previous_game_highDangerShotsFor", "matchup_average_game_reboundsFor_last_3_games", "matchup_average_player_EvenStrengthShotAttemptsInGame_last_3_games", "matchup_previous_game_penaltiesAgainst", "matchup_average_game_reboundGoalsAgainst_last_7_games", "average_game_mediumDangerShotsFor_last_7_games", "previous_game_shotAttemptsFor", "average_game_takeawaysFor_last_3_games", "average_game_playContinuedOutsideZoneAgainst_last_7_games", "matchup_average_game_faceOffsWonFor_last_3_games", "average_game_lowDangerShotsFor_last_7_games", "matchup_average_game_takeawaysFor_last_3_games", "average_game_goalsAgainst_last_3_games", "previous_game_shotAttemptsAgainst", "average_game_savedShotsOnGoalAgainst_last_7_games", "average_game_reboundsAgainst_last_3_games", "average_player_totalTimeOnIceSinceFaceoffInGame_last_3_games", "average_game_reboundsAgainst_last_7_games", "average_game_savedUnblockedShotAttemptsAgainst_last_7_games", "average_player_ShotsOnReboundsInGame_last_7_games", "matchup_average_player_PenaltyKillShotsInGame_last_7_games", "matchup_average_player_GoalsInGame_last_7_games", "previous_game_playContinuedOutsideZoneAgainst", "previous_game_blockedShotAttemptsFor", "average_game_penaltiesAgainst_last_7_games", "average_game_playContinuedInZoneAgainst_last_3_games", "matchup_average_player_EvenStrengthShotsInGame_last_7_games", "matchup_average_game_shotAttemptsAgainst_last_3_games", "matchup_average_game_blockedShotAttemptsAgainst_last_7_games", "matchup_average_game_corsiPercentage_last_3_games", "average_player_ShotAttemptsInGame_last_3_games", "matchup_average_game_shotAttemptsAgainst_last_7_games", "previous_player_avgShotDistanceInGame", "average_game_playContinuedOutsideZoneFor_last_3_games", "matchup_average_game_shotsOnGoalAgainst_last_7_games", "playerTeam", "matchup_previous_player_PenaltyKillShotsInGame", "average_player_PenaltyKillShotAttemptsInGame_last_7_games", "matchup_previous_game_playContinuedOutsideZoneFor", "shooterName", "matchup_average_game_playContinuedInZoneFor_last_3_games", "matchup_previous_game_shotsOnGoalFor", "matchup_previous_game_playContinuedOutsideZoneAgainst", "average_player_totalTimeOnIceInGame_last_3_games", "matchup_average_game_reboundsFor_last_7_games", "matchup_average_player_PowerPlayShotAttemptsInGame_last_3_games", "matchup_previous_game_playContinuedInZoneAgainst", "average_game_lowDangerShotsAgainst_last_3_games", "matchup_average_game_lowDangerShotsFor_last_7_games", "previous_player_PenaltyKillShotAttemptsInGame", "previous_player_ShotsOnGoalInGame", "previous_player_totalTimeOnIceSinceFaceoffInGame", "matchup_average_player_ShotsOnEmptyNetInGame_last_3_games", "matchup_average_game_mediumDangerShotsFor_last_7_games", "matchup_average_player_totalTimeOnIceInGame_last_7_games", "matchup_average_game_missedShotsAgainst_last_3_games", "matchup_average_game_shotAttemptsFor_last_3_games", "matchup_previous_game_playContinuedInZoneFor", "average_player_EvenStrengthShotsInGame_last_7_games", "matchup_previous_player_PowerPlayShotAttemptsInGame", "matchup_previous_game_giveawaysAgainst", "average_game_takeawaysAgainst_last_7_games", "matchup_average_game_lowDangerShotsAgainst_last_3_games", "matchup_previous_player_PenaltyKillShotAttemptsInGame", "average_game_savedShotsOnGoalFor_last_3_games", "average_player_EvenStrengthShotsInGame_last_3_games", "matchup_previous_game_reboundsFor", "previous_game_reboundsFor", "previous_player_PowerPlayShotAttemptsInGame", "average_game_shotsOnGoalFor_last_7_games", "average_player_ShotsOnRushesInGame_last_3_games", "average_game_highDangerShotsFor_last_7_games", "matchup_average_game_goalsAgainst_last_7_games", "matchup_previous_player_GoalsInGame", "matchup_average_game_reboundGoalsAgainst_last_3_games", "average_game_goalPercentageFor_last_7_games", "previous_game_corsiPercentage", "matchup_average_player_totalTimeOnIceSinceFaceoffInGame_last_3_games", "average_player_ShotsOnGoalInGame_last_7_games", "playerGamesPlayedRolling", "average_game_faceOffsWonAgainst_last_7_games", "average_player_avgSpeedFromLastEvent_last_3_games", "average_player_GoalsInGame_last_7_games", "matchup_previous_game_lowDangerShotsFor", "average_game_giveawaysAgainst_last_3_games", "previous_game_lowDangerShotsFor", "matchup_average_game_highDangerShotsAgainst_last_3_games", "matchup_previous_game_corsiPercentage", "average_game_penaltiesFor_last_7_games", "average_game_hitsFor_last_3_games", "average_game_corsiPercentage_last_7_games", "matchup_previous_game_faceOffsWonFor", "average_game_reboundGoalsAgainst_last_3_games", "previous_game_giveawaysFor", "previous_player_ShotAttemptsInGame", "matchup_average_game_hitsAgainst_last_3_games", "matchup_average_game_playContinuedInZoneAgainst_last_7_games", "matchup_previous_game_reboundsAgainst", "average_game_highDangerShotsFor_last_3_games", "matchup_previous_game_hitsFor", "average_game_reboundsFor_last_3_games", "average_game_shotsOnGoalAgainst_last_3_games", "matchup_previous_game_savedUnblockedShotAttemptsAgainst", "average_game_goalPercentageAgainst_last_7_games", "average_game_takeawaysAgainst_last_3_games", "average_game_missedShotsFor_last_3_games", "matchup_previous_game_goalsAgainst", "playerMatchupPlayedRolling", "matchup_previous_game_blockedShotAttemptsFor", "matchup_average_game_penaltiesAgainst_last_3_games", "average_player_ShotAttemptsInGame_last_7_games", "average_game_lowDangerShotsFor_last_3_games", "average_player_PenaltyKillShotAttemptsInGame_last_3_games", "average_player_PenaltyKillShotsInGame_last_7_games", "average_game_blockedShotAttemptsFor_last_7_games", "average_game_shotAttemptsFor_last_3_games", "matchup_average_game_playContinuedOutsideZoneFor_last_3_games", "matchup_average_game_playContinuedOutsideZoneFor_last_7_games", "matchup_average_game_reboundGoalsFor_last_7_games", "previous_game_goalsAgainst", "matchup_average_game_mediumDangerShotsAgainst_last_7_games", "matchup_average_game_highDangerShotsFor_last_3_games", "matchup_average_game_reboundsAgainst_last_3_games", "previous_game_missedShotsAgainst", "matchup_average_game_savedShotsOnGoalFor_last_3_games", "matchup_previous_game_giveawaysFor", "average_player_ShotsOnEmptyNetInGame_last_3_games", "matchup_previous_player_ShotsOnEmptyNetInGame", "matchup_average_game_penaltiesFor_last_7_games", "matchup_previous_game_blockedShotAttemptsAgainst", "previous_game_savedUnblockedShotAttemptsFor", "previous_game_hitsFor", "matchup_average_game_fenwickPercentage_last_3_games", "average_game_reboundGoalsFor_last_7_games", "average_game_blockedShotAttemptsAgainst_last_7_games", "average_game_mediumDangerShotsAgainst_last_7_games", "matchup_previous_game_takeawaysFor", "average_game_shotAttemptsAgainst_last_7_games", "matchup_average_player_ShotsOnGoalInGame_last_3_games", "teamGamesPlayedRolling", "average_game_missedShotsFor_last_7_games", "average_game_lowDangerShotsAgainst_last_7_games", "rolling_playerTotalTimeOnIceInGame", "matchup_average_player_PenaltyKillShotAttemptsInGame_last_7_games", "matchup_average_game_takeawaysFor_last_7_games", "average_game_faceOffsWonAgainst_last_3_games", "matchup_previous_game_reboundGoalsFor", "matchup_previous_game_missedShotsAgainst", "matchup_previous_game_faceOffsWonAgainst", "average_game_goalsFor_last_3_games", "previous_player_PowerPlayShotsInGame", "previous_game_penaltiesFor", "dummyDay", "matchup_previous_player_totalTimeOnIceSinceFaceoffInGame", "matchup_average_game_corsiPercentage_last_7_games", "matchup_average_game_savedUnblockedShotAttemptsFor_last_7_games", "playerId", "matchup_previous_game_fenwickPercentage", "average_player_ShotsOnGoalInGame_last_3_games", "average_game_hitsAgainst_last_7_games", "previous_player_totalTimeOnIceInGame", "average_game_corsiPercentage_last_3_games", "average_game_goalsAgainst_last_7_games", "average_player_avgShotDistanceInGame_last_3_games", "average_game_penaltiesAgainst_last_3_games", "matchup_previous_game_hitsAgainst", "average_player_EvenStrengthShotAttemptsInGame_last_7_games", "average_player_PowerPlayShotsInGame_last_3_games", "average_game_missedShotsAgainst_last_3_games", "matchup_previous_game_reboundGoalsAgainst", "average_player_ShotsOnRushesInGame_last_7_games", "previous_opposingTeam", "previous_game_reboundGoalsAgainst", "previous_game_playContinuedInZoneAgainst", "average_game_faceOffsWonFor_last_3_games", "matchup_average_game_blockedShotAttemptsAgainst_last_3_games", "matchup_average_player_ShotAttemptsInGame_last_7_games", "matchup_average_game_goalsAgainst_last_3_games", "previous_game_playContinuedOutsideZoneFor", "previous_game_faceOffsWonAgainst", "average_player_avgSpeedFromLastEvent_last_7_games", "matchup_average_player_avgSpeedFromLastEvent_last_3_games", "previous_game_goalPercentageAgainst", "previous_game_shotsOnGoalFor", "matchup_average_game_savedShotsOnGoalFor_last_7_games", "average_player_PowerPlayShotAttemptsInGame_last_3_games", "average_game_mediumDangerShotsFor_last_3_games", "matchup_previous_game_savedShotsOnGoalAgainst", "previous_game_reboundGoalsFor", "matchup_average_game_hitsAgainst_last_7_games", "matchup_previous_game_missedShotsFor", "previous_game_reboundsAgainst", "previous_game_mediumDangerShotsAgainst", "previous_game_missedShotsFor", "previous_game_shotsOnGoalAgainst", "matchup_previous_player_PowerPlayShotsInGame", "average_player_ShotsOnEmptyNetInGame_last_7_games", "average_player_ShotsOnReboundsInGame_last_3_games", "matchup_previous_player_ShotsOnGoalInGame", "previous_player_GoalsInGame", "matchup_average_game_takeawaysAgainst_last_3_games", "matchup_average_game_missedShotsFor_last_3_games", "matchup_average_game_savedUnblockedShotAttemptsFor_last_3_games", "matchup_previous_game_shotAttemptsFor", "previous_game_mediumDangerShotsFor", "previous_player_ShotsOnReboundsInGame", "previous_game_playContinuedInZoneFor", "previous_player_EvenStrengthShotAttemptsInGame", "matchup_average_game_missedShotsAgainst_last_7_games", "average_player_PowerPlayShotsInGame_last_7_games", "matchup_average_game_shotsOnGoalFor_last_7_games", "average_game_giveawaysFor_last_3_games", "matchup_average_game_penaltiesFor_last_3_games", "average_game_playContinuedInZoneFor_last_3_games", "average_game_shotAttemptsAgainst_last_3_games", "average_game_savedShotsOnGoalFor_last_7_games", "matchup_average_game_playContinuedInZoneFor_last_7_games", "previous_player_avgSpeedFromLastEvent", "matchup_average_game_lowDangerShotsFor_last_3_games", "isHome", "average_player_totalTimeOnIceInGame_last_7_games", "matchup_average_game_takeawaysAgainst_last_7_games", "matchup_previous_game_penaltiesFor", "average_player_GoalsInGame_last_3_games", "average_player_avgShotDistanceInGame_last_7_games", "matchup_average_game_giveawaysAgainst_last_3_games", "matchup_average_player_ShotsOnRushesInGame_last_7_games", "average_game_hitsFor_last_7_games", "matchup_average_player_avgShotDistanceInGame_last_7_games", "average_game_reboundGoalsFor_last_3_games", "average_game_savedUnblockedShotAttemptsAgainst_last_3_games", "matchup_average_game_playContinuedOutsideZoneAgainst_last_7_games", "matchup_previous_game_goalPercentageAgainst", "matchup_previous_player_avgSpeedFromLastEvent", "matchup_average_player_ShotsOnReboundsInGame_last_7_games", "matchup_previous_game_goalPercentageFor", "matchup_average_game_blockedShotAttemptsFor_last_7_games", "matchup_average_game_shotsOnGoalAgainst_last_3_games", "matchup_average_game_playContinuedOutsideZoneAgainst_last_3_games", "previous_game_fenwickPercentage", "matchup_previous_game_lowDangerShotsAgainst", "average_game_shotsOnGoalFor_last_3_games", "previous_player_ShotsOnEmptyNetInGame", "average_game_highDangerShotsAgainst_last_3_games", "matchup_average_game_goalPercentageFor_last_7_games", "previous_game_faceOffsWonFor", "matchup_average_player_PowerPlayShotsInGame_last_3_games", "matchup_average_player_PenaltyKillShotsInGame_last_3_games", "matchup_average_game_reboundGoalsFor_last_3_games", "average_game_reboundGoalsAgainst_last_7_games", "matchup_average_game_lowDangerShotsAgainst_last_7_games", "matchup_average_game_highDangerShotsFor_last_7_games", "average_game_playContinuedOutsideZoneAgainst_last_3_games", "previous_game_savedShotsOnGoalFor", "average_game_savedShotsOnGoalAgainst_last_3_games", "previous_game_savedUnblockedShotAttemptsAgainst", "matchup_average_game_goalPercentageAgainst_last_7_games", "matchup_average_player_avgSpeedFromLastEvent_last_7_games", "matchup_previous_player_EvenStrengthShotAttemptsInGame", "matchup_average_game_savedShotsOnGoalAgainst_last_7_games", "matchup_average_player_EvenStrengthShotsInGame_last_3_games", "average_game_playContinuedInZoneAgainst_last_7_games", "matchup_average_game_faceOffsWonAgainst_last_7_games", "average_game_faceOffsWonFor_last_7_games", "gameDate", "matchup_average_game_faceOffsWonAgainst_last_3_games", "matchup_average_player_totalTimeOnIceInGame_last_3_games", "previous_game_takeawaysAgainst", "matchup_average_game_hitsFor_last_7_games", "matchup_average_game_giveawaysFor_last_3_games", "matchup_previous_player_avgShotDistanceInGame", "matchup_previous_player_ShotsOnReboundsInGame", "matchup_average_game_highDangerShotsAgainst_last_7_games", "matchup_average_game_goalPercentageAgainst_last_3_games", "average_player_totalTimeOnIceSinceFaceoffInGame_last_7_games", "previous_game_savedShotsOnGoalAgainst", "matchup_average_game_missedShotsFor_last_7_games", "matchup_previous_game_shotAttemptsAgainst", "average_game_takeawaysFor_last_7_games", "matchup_previous_game_savedUnblockedShotAttemptsFor", "average_game_goalPercentageFor_last_3_games", "average_game_giveawaysAgainst_last_7_games", "matchup_average_game_giveawaysAgainst_last_7_games", "matchup_previous_game_highDangerShotsAgainst", "average_game_goalsFor_last_7_games", "matchup_average_game_reboundsAgainst_last_7_games", "matchup_average_game_savedUnblockedShotAttemptsAgainst_last_3_games", "matchup_previous_player_totalTimeOnIceInGame", "average_game_fenwickPercentage_last_7_games", "average_game_reboundsFor_last_7_games", "matchup_average_player_ShotsOnGoalInGame_last_7_games", "matchup_average_player_totalTimeOnIceSinceFaceoffInGame_last_7_games", "matchup_average_game_playContinuedInZoneAgainst_last_3_games", "matchup_previous_player_EvenStrengthShotsInGame", "matchup_average_game_mediumDangerShotsFor_last_3_games", "matchup_average_game_fenwickPercentage_last_7_games", "matchup_average_game_mediumDangerShotsAgainst_last_3_games", "average_game_playContinuedInZoneFor_last_7_games", "matchup_previous_game_mediumDangerShotsFor", "previous_game_goalsFor", "matchup_average_game_savedShotsOnGoalAgainst_last_3_games", "average_game_savedUnblockedShotAttemptsFor_last_7_games", "matchup_average_game_penaltiesAgainst_last_7_games", "average_game_savedUnblockedShotAttemptsFor_last_3_games", "matchup_average_player_ShotsOnEmptyNetInGame_last_7_games", "average_game_missedShotsAgainst_last_7_games", "average_game_highDangerShotsAgainst_last_7_games", "matchup_average_player_EvenStrengthShotAttemptsInGame_last_7_games", "matchup_previous_game_mediumDangerShotsAgainst", "matchup_previous_game_goalsFor", "previous_game_highDangerShotsAgainst", "matchup_previous_game_highDangerShotsFor", "average_player_PowerPlayShotAttemptsInGame_last_7_games", "average_player_EvenStrengthShotAttemptsInGame_last_3_games", "average_game_penaltiesFor_last_3_games", "matchup_previous_player_ShotAttemptsInGame", "matchup_average_player_ShotAttemptsInGame_last_3_games", "matchup_previous_game_savedShotsOnGoalFor", "matchup_average_player_PenaltyKillShotAttemptsInGame_last_3_games", "average_game_shotsOnGoalAgainst_last_7_games", "average_game_playContinuedOutsideZoneFor_last_7_games", "matchup_average_player_avgShotDistanceInGame_last_3_games", "matchup_average_game_goalsFor_last_7_games", "average_game_hitsAgainst_last_3_games", "matchup_average_game_shotAttemptsFor_last_7_games", "previous_game_giveawaysAgainst", "matchup_average_game_blockedShotAttemptsFor_last_3_games", "average_game_mediumDangerShotsAgainst_last_3_games", "matchup_average_game_savedUnblockedShotAttemptsAgainst_last_7_games", "matchup_average_game_goalPercentageFor_last_3_games", "opposingTeam", "matchup_average_player_GoalsInGame_last_3_games", "matchup_average_game_goalsFor_last_3_games", "matchup_average_game_faceOffsWonFor_last_7_games", "previous_game_lowDangerShotsAgainst", "previous_game_penaltiesAgainst", "average_game_shotAttemptsFor_last_7_games", "average_player_PenaltyKillShotsInGame_last_3_games", "average_game_blockedShotAttemptsFor_last_3_games", "matchup_average_player_ShotsOnReboundsInGame_last_3_games"]
# col_selector = ColumnSelector(supported_cols)

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
        f"SELECT COUNT(DISTINCT {col}) as count FROM lr_nhl_demo.dev.gold_model_stats_delta_v2"
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

feature_union = FeatureUnion(
    [
        ("mi_pipeline", mi_pipeline),
        ("rfe_pipeline", rfe_pipeline),
        ("rf_pipeline", rf_pipeline),
    ]
)

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
from sklearn.model_selection import train_test_split

# Convert df_loaded to Pandas DataFrame
df_loaded_pd = df_loaded.toPandas()

# Separate target column from features
X = df_loaded_pd.drop([target_col], axis=1)
y = df_loaded_pd[target_col]

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

# # Separate target column from features and drop _automl_split_col_0000
# X_train = split_train_df.drop([target_col], axis=1)
# y_train = split_train_df[target_col]

# X_val = split_val_df.drop([target_col], axis=1)
# y_val = split_val_df[target_col]

# X_test = split_test_df.drop([target_col], axis=1)
# y_test = split_test_df[target_col]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Test Feature Selection Code

# COMMAND ----------

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

# # Create the full pipeline
# features_pipeline = Pipeline(
#     [
#         ("column_selector", col_selector),
#         ("preprocessor", preprocessor),
#         (
#             "feature_selector",
#             SelectFromModel(RandomForestRegressor(n_estimators=5, random_state=42)),
#         ),
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

# DBTITLE 1,feature selection testing
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import RFE, RFECV
from lightgbm import LGBMRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.datasets import make_regression
from sklearn.model_selection import RepeatedKFold

# Initialize the LGBMRegressor
lgbm = LGBMRegressor(random_state=42)

# Set up RFECV
rfecv = RFECV(estimator=lgbm, step=20, cv=RepeatedKFold(n_splits=3, n_repeats=2, random_state=42), scoring='neg_mean_squared_error', min_features_to_select=100, verbose=1)

# Create a pipeline with RFECV
pipeline_rfecv = Pipeline([
    ('column_selector', col_selector),
    ('preprocessor', preprocessor),
    ('feature_selection_rfecv', rfecv),
    ('model', lgbm)
])

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

set_config(transform_output="pandas")

features_pipeline.fit(X_train, y_train)


# COMMAND ----------

processed_X_train = features_pipeline.transform(X_train)

processed_X_train

# COMMAND ----------

# Get selected columns
selected_columns = features_pipeline.named_steps["feature_selection_rfr"].get_support()
print("Selected feature names count:", len(selected_columns))

# TO DO: convert X_train below to be the preprocessed dataset
# feature_name = X_train.columns[selected_columns]
# feature_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train regression model
# MAGIC - Log relevant metrics to MLflow to track runs
# MAGIC - All the runs are logged under [this MLflow experiment](#mlflow/experiments/2824690123542843)
# MAGIC - Change the model parameters and re-run the training cell to log a different trial to the MLflow experiment
# MAGIC - To view the full list of tunable hyperparameters, check the output of the cell below

# COMMAND ----------

help(LGBMRegressor)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the objective function
# MAGIC The objective function used to find optimal hyperparameters. By default, this notebook only runs
# MAGIC this function once (`max_evals=1` in the `hyperopt.fmin` invocation) with fixed hyperparameters, but
# MAGIC hyperparameters can be tuned by modifying `space`, defined below. `hyperopt.fmin` will then use this
# MAGIC function's return value to search the space to minimize the loss.

# COMMAND ----------

# DBTITLE 1,log preprocessing pipeline
import mlflow
import os
from mlflow.models import Model, infer_signature, ModelSignature
from mlflow.pyfunc import PythonModel
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import SelectFromModel
import joblib


class PreprocessModel(PythonModel):
    def __init__(self, pipeline):
        self.pipeline = pipeline

    def fit(self, X, y):
        self.pipeline.fit(X, y)

    def predict(self, context, model_input):
        return self.pipeline.transform(model_input)


# Create the preprocessing pipeline
preprocess_pipeline = Pipeline(
    [
        ("column_selector", col_selector),
        ("preprocessor", preprocessor),
        (
            "feature_selector",
            SelectFromModel(RandomForestRegressor(n_estimators=5, random_state=42)),
        ),
    ]
)

# Create an instance of the custom PythonModel
pyfunc_preprocess_model = PreprocessModel(preprocess_pipeline)

# Fit the model
pyfunc_preprocess_model.fit(X_train, y_train)

# Get the current working directory
cwd = os.getcwd()

# Specify the file name
file_name = 'preprocess_model'

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

# Save the model using MLflow
with mlflow.start_run() as run:
    mlflow.pyfunc.save_model(
        path="preprocess_model",
        python_model=pyfunc_preprocess_model,
        input_example=X_train.iloc[:5],
        signature=infer_signature(X_train, preprocess_pipeline.transform(X_train)),
    )

    # Log the model
    mlflow.pyfunc.log_model(
        artifact_path="preprocess_model",
        python_model=pyfunc_preprocess_model,
        input_example=X_train.iloc[:5],
        signature=infer_signature(X_train, preprocess_pipeline.transform(X_train)),
    )

# Load the saved model
loaded_model = mlflow.pyfunc.load_model("preprocess_model")

# Transform datasets using the loaded model
X_val_processed = loaded_model.predict(X_val)
X_train_processed = loaded_model.predict(X_train)
X_test_processed = loaded_model.predict(X_test)

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

# COMMAND ----------

# Get the run ID and model URI
preprocess_model_uri = f"runs:/{preprocess_run.info.run_id}/preprocess_model"

# Register the model
mlflow.register_model(preprocess_model_uri, "lr_nhl_demo.dev.preprocess_model")

# COMMAND ----------

# Set as Champion based on max version
client = mlflow.tracking.MlflowClient()
model_version_infos = client.search_model_versions("name = 'lr_nhl_demo.dev.preprocess_model'")
new_model_version = max([model_version_info.version for model_version_info in model_version_infos])
client.set_registered_model_alias("lr_nhl_demo.dev.preprocess_model", "champion", new_model_version)

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

X_train_processed_UC = mlflow.data.load_delta(table_name="lr_nhl_demo.dev.X_train_processed", version="0")
X_train_processed_UC_PD = X_train_processed_UC.df.toPandas()

X_train_processed_UC_PD

# COMMAND ----------

import mlflow
from mlflow.models import Model, infer_signature, ModelSignature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
from hyperopt import hp, tpe, fmin, STATUS_OK, SparkTrials
import pyspark.pandas as ps

# Write X_train_processed to Unity Catalog for lineage tracking
ps.from_pandas(X_train_processed).to_table("lr_nhl_demo.dev.X_train_processed", mode="overwrite")
print("Writing X_train_processed to UC!")

# Load a Unity Catalog table, train a model, and log the input table
print("Loading X_train_processed_UC")
X_train_processed_UC = mlflow.data.load_delta(table_name="lr_nhl_demo.dev.X_train_processed", version="0")
X_train_processed_UC_PD = X_train_processed_UC.df.toPandas()

import mlflow
from sklearn.pipeline import Pipeline
from lightgbm import LGBMRegressor
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from hyperopt import fmin, tpe, hp, STATUS_OK
from hyperopt.pyll.base import scope
from mlflow.models import Model
from mlflow.pyfunc import PyFuncModel

# Define the objective function to accept different model types
def objective(params):
    model_type = params.pop('model_type')

    with mlflow.start_run(experiment_id="634720160613016") as mlflow_run:
        mlflow.set_tag("model_type", model_type)
        
        # Select the model based on the model_type parameter
        if model_type == 'lightgbm':
            model = LGBMRegressor(**params)
            print(f'Training LightGBM model with params: {params}')
        elif model_type == 'linear':
            model = LinearRegression()
            print(f'Training LinearRegression model with params: {params}')
        elif model_type == 'random_forest':
            model = RandomForestRegressor(**params)
            print(f'Training RandomForestRegressor model with params: {params}')
        elif model_type == 'xgboost':
            model = XGBRegressor(**params)
            print(f'Training XGBRegressor model with params: {params}')
        else:
            raise ValueError(f'Unknown model type: {model_type}')
        
        # Create a pipeline with the selected model
        pipeline = Pipeline(
            [
                ('regressor', model),
            ]
        )

        # Enable automatic logging of input samples, metrics, parameters, and models
        mlflow.sklearn.autolog(
            log_input_examples=True,
            silent=True,
        )

        pipeline.fit(
            X_train_processed,
            y_train,
            # These callbacks are specific to LightGBM, so they should only be used for LightGBM
            **({'regressor__callbacks': [lightgbm.early_stopping(5), lightgbm.log_evaluation(0)],
                'regressor__eval_set': [(X_val_processed, y_val)]} if model_type == 'lightgbm' else {})
        )
        
        # Log metrics for the training set
        mlflow_model = Model()
        pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
        pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=pipeline)
        training_eval_result = mlflow.evaluate(
            model=pyfunc_model,
            data=X_train_processed.assign(**{str(target_col): y_train}),
            targets=target_col,
            model_type="regressor",
            evaluator_config={
                'log_model_explainability': False,
                'metric_prefix': 'training_',
            },
        )
        
        # Log metrics for the validation set
        val_eval_result = mlflow.evaluate(
            model=pyfunc_model,
            data=X_val_processed.assign(**{str(target_col): y_val}),
            targets=target_col,
            model_type="regressor",
            evaluator_config={
                'log_model_explainability': False,
                'metric_prefix': 'val_',
            },
        )
        val_metrics = val_eval_result.metrics
        
        # Log metrics for the test set
        test_eval_result = mlflow.evaluate(
            model=pyfunc_model,
            data=X_test_processed.assign(**{str(target_col): y_test}),
            targets=target_col,
            model_type="regressor",
            evaluator_config={
                'log_model_explainability': False,
                'metric_prefix': 'test_',
            },
        )
        test_metrics = test_eval_result.metrics

        loss = -val_metrics["val_r2_score"]

        # Truncate metric key names so they can be displayed together
        val_metrics = {
            k.replace("val_", ""): v for k, v in val_metrics.items()
        }
        test_metrics = {
            k.replace("test_", ""): v for k, v in test_metrics.items()
        }

        return {
            "loss": loss,
            "status": STATUS_OK,
            "val_metrics": val_metrics,
            "test_metrics": test_metrics,
            "model": pipeline,
            "run": mlflow_run,
        }

X_train_processed

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
space = hp.choice('classifier_type', [
    {
        'model_type': 'lightgbm',
        "colsample_bytree": hp.uniform("colsample_bytree", 0.5, 1.0),
        "lambda_l1": hp.loguniform("lambda_l1", -5, 0),
        "lambda_l2": hp.loguniform("lambda_l2", -5, 2),
        "learning_rate": hp.loguniform("learning_rate", -5, -1),
        "max_bin": scope.int(hp.quniform("max_bin", 20, 100, 1)),
        "max_depth": scope.int(hp.quniform("max_depth", 3, 15, 1)),
        "min_child_samples": scope.int(hp.quniform("min_child_samples", 20, 200, 1)),
        "n_estimators": scope.int(hp.quniform("n_estimators", 100, 500, 1)),
        "num_leaves": scope.int(hp.quniform("num_leaves", 31, 255, 1)),
        "subsample": hp.uniform("subsample", 0.5, 1.0),
        "random_state": 729986891,
    },
    # {
    #     'model_type': 'linear',
    # },
    {
        'model_type': 'random_forest',
        "n_estimators": scope.int(hp.quniform("rf_n_estimators", 100, 500, 1)),
        "max_depth": scope.int(hp.quniform("rf_max_depth", 3, 15, 1)),
        "min_samples_split": hp.uniform("rf_min_samples_split", 0.1, 1.0),
        "min_samples_leaf": hp.uniform("rf_min_samples_leaf", 0.1, 0.5),
        "random_state": 729986891,
    },
    {
        'model_type': 'xgboost',
        "n_estimators": scope.int(hp.quniform("xgb_n_estimators", 100, 500, 1)),
        "max_depth": scope.int(hp.quniform("xgb_max_depth", 3, 15, 1)),
        "learning_rate": hp.loguniform("xgb_learning_rate", -5, -1),
        "subsample": hp.uniform("xgb_subsample", 0.5, 1.0),
        "colsample_bytree": hp.uniform("xgb_colsample_bytree", 0.5, 1.0),
        "random_state": 729986891,
    }
])

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

trials = SparkTrials()

# Run the optimization
fmin(
    fn=objective,
    space=space,
    algo=tpe.suggest,
    max_evals=100,
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

# Retrain the best model with the best hyperparameters on the FULL dataset
# # Retrieve the best model and parameters
# best_trial = trials.best_trial['result']
# best_model_type = best_trial['model']
# best_params = best_trial['params']

# # Retrain the best model on the full dataset
# X_full = ...  # Combine your training, validation, and test sets
# y_full = ...  # Combine your target values

# with mlflow.start_run(experiment_id="634720160613016") as final_run:
#     # Set tags for the final run
#     mlflow.set_tag("model_type", best_model_type)
#     mlflow.set_tag("experiment", "final_model_training")

#     # Retrain the model
#     final_model = Pipeline([('regressor', best_model_type)])
#     final_model.fit(X_full, y_full)

#     # Log the final model
#     mlflow.sklearn.log_model(final_model, "final_model")

#     print(f"Final model trained with best parameters: {best_params}")

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
        n=min(100, X_train_processed.shape[0]), random_state=729986891
    ).fillna(mode)

    # Sample some rows from the validation set to explain. Increase the sample size for more thorough results.
    example = X_val_processed.sample(
        n=min(100, X_val_processed.shape[0]), random_state=729986891
    ).fillna(mode)

    # Use Kernel SHAP to explain feature importance on the sampled rows from the validation set.
    predict = lambda x: model.predict(
        pd.DataFrame(x, columns=X_train_processed.columns)
    )
    explainer = KernelExplainer(predict, train_sample, link="identity")
    shap_values = explainer.shap_values(example, l1_reg=False, nsamples=500)
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
print(f'Successfully set the best_model_uri to {best_model_uri}')

# COMMAND ----------


