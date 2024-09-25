# Databricks notebook source
import mlflow
from pyspark.sql.functions import col

target_col = "player_Total_shotsOnGoal"
time_col = "gameDate"
catalog = 'lr_nhl_demo'
schema = 'dev'

# COMMAND ----------

gold_model_stats = spark.table("lr_nhl_demo.dev.gold_model_stats_delta_v2")

# COMMAND ----------

# DBTITLE 1,define main dataframe
model_remove_1st_and_upcoming_games = gold_model_stats.filter(
    (col("gameId").isNotNull())
    # & (col("playerGamesPlayedRolling") > 0)
    & (col("rolling_playerTotalTimeOnIceInGame") > 180)
)

df_loaded = model_remove_1st_and_upcoming_games

model_remove_1st_and_upcoming_games.count()

# COMMAND ----------

# DBTITLE 1,Ensure Dataframe is Unique
assert (
    model_remove_1st_and_upcoming_games.count()
    == model_remove_1st_and_upcoming_games.select("gameId", "playerId")
    .distinct()
    .count()
)

# COMMAND ----------

# DBTITLE 1,Write Dataframe to UC
spark.sql("DROP TABLE IF EXISTS lr_nhl_demo.dev.pre_feat_eng")

df_loaded.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.pre_feat_eng")
print(f"Successfully solidified {catalog}.{schema}.pre_feat_eng to --> {catalog}.{schema}.pre_feat_eng")

# COMMAND ----------

upcoming_games = gold_model_stats.filter(
    (col("gameId").isNull())
    # & (col("playerGamesPlayedRolling") > 0)
    & (col("rolling_playerTotalTimeOnIceInGame") > 180)
    & (col("gameDate") != "2024-01-17")
)

display(upcoming_games)

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
from mlflow.models import infer_signature
from mlflow.pyfunc import PythonModel
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import SelectFromModel
from databricks.feature_store import FeatureStoreClient
from sklearn import set_config


class PreprocessModel(PythonModel):
    def __init__(self, pipeline, id_columns):
        self.pipeline = pipeline
        self.id_columns = id_columns

    def fit(self, X, y):
        self.pipeline.fit(X.drop(columns=self.id_columns), y)

    def predict(self, context, model_input):
        id_data = model_input[self.id_columns]
        transformed_data = self.pipeline.transform(
            model_input.drop(columns=self.id_columns)
        )

        # Check if transformed_data is a DataFrame
        if not isinstance(transformed_data, pd.DataFrame):
            # Convert transformed_data to DataFrame if it's not already
            raise ValueError("transformed_data must be a DataFrame.", transformed_data)

        # Ensure index alignment before concatenation
        transformed_data.index = id_data.index

        # Concatenate id_data with transformed_data
        final_data = pd.concat([id_data, transformed_data], axis=1)

        return final_data


def create_feature_store_tables(
    X_train, y_train, col_selector, preprocessor, n_estimators, feature_counts
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

    mlflow.end_run()
    fs = FeatureStoreClient()
    id_columns = ["gameId", "playerId"]

    # Create a single preprocessing pipeline with a configurable feature selector
    preprocess_pipeline = Pipeline(
        [
            ("column_selector", col_selector),
            ("preprocessor", preprocessor),
            (
                "feature_selector",
                SelectFromModel(
                    RandomForestRegressor(n_estimators=n_estimators, random_state=42),
                    max_features=feature_counts,  # We'll configure this later
                    threshold=-np.inf,
                ),
            ),
        ]
    )

    # Fit the pipeline on the full dataset, excluding id columns
    preprocess_pipeline.fit(X_train.drop(columns=id_columns), y_train)

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
    with mlflow.start_run() as run:
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

    uc_model_name = f"lr_nhl_demo.dev.{file_name}"

    # Register the model
    print(f"Registering model {uc_model_name}...")
    mlflow.register_model(artifact_uri_base_path, uc_model_name)

    # Set as Champion
    print("Setting model as Champion...")
    client = mlflow.tracking.MlflowClient()
    model_version_infos = client.search_model_versions(f"name = '{uc_model_name}'")
    new_model_version = max(
        [model_version_info.version for model_version_info in model_version_infos]
    )
    client.set_registered_model_alias(uc_model_name, "champion", new_model_version)

    pp_champion_version = client.get_model_version_by_alias(uc_model_name, "champion")

    preprocess_model_name = pp_champion_version.name
    preprocess_model_version = pp_champion_version.version

    preprocess_model_uri = f"models:/{preprocess_model_name}/{preprocess_model_version}"
    preprocess_model = mlflow.pyfunc.load_model(model_uri=preprocess_model_uri)

    mlflow.pyfunc.get_model_dependencies(preprocess_model_uri)

    print(f"Creating Feature Store table for {feature_counts} features...")

    # Transform datasets using the configured model
    X_train_processed = pyfunc_preprocess_model.predict(None, X_train)

    X_train_processed_spark = spark.createDataFrame(X_train_processed)

    display(X_train_processed_spark)

    table_name = f"lr_nhl_demo.dev.player_features_{feature_counts}"

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
        mode="overwrite",
    )

    print(f"Feature Store table {table_name} created SUCCESSFULLY...")


# COMMAND ----------

# Convert df_loaded to Pandas DataFrame
df_loaded_pd = df_loaded.toPandas()

# Separate target column from features
X = df_loaded_pd.drop([target_col], axis=1)
y = df_loaded_pd[target_col]

# COMMAND ----------

# feature_counts_param = dbutils.widgets.get("train_model_param")
feature_counts = [25, 50, 100, 200]
for count in feature_counts:
    print(f"Feature Engineering Pipeline RUNNING on {count} features")
    create_feature_store_tables(X, y, col_selector, preprocessor, 5, count)
    print(f"Feature Engineering Pipeline COMPLETE on {count} features")

# COMMAND ----------

train_model_param = dbutils.widgets.get("train_model_param").lower()

if train_model_param == "true":
    # Set training task condition to true/false for next pipeline steps
    dbutils.jobs.taskValues.set(key="train_model", value="true")
else:
    dbutils.jobs.taskValues.set(key="train_model", value="false")
