# Databricks notebook source
# Imports
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

games_v2 = spark.table("dev.bronze_games_historical_v2")
player_game_stats = spark.table("dev.bronze_player_game_stats")
player_game_stats_v2 = spark.table("dev.bronze_player_game_stats_v2")
bronze_schedule_2023_v2 = spark.table("dev.bronze_schedule_2023_v2")

schedule_2023 = spark.table("dev.2023_24_official_nhl_schedule_by_day")
silver_games_schedule_v2 = spark.table("dev.silver_games_schedule_v2")

silver_games_historical_v2 = spark.table("dev.silver_games_historical_v2")
gold_player_stats_v2 = spark.table("dev.gold_player_stats_v2")
gold_merged_stats_v2 = spark.table("dev.gold_merged_stats_v2")
gold_game_stats_v2 = spark.table("dev.gold_game_stats_v2")
gold_model_data_v2 = spark.table("dev.gold_model_stats_v2")

# COMMAND ----------

display(silver_games_schedule_v2)

# COMMAND ----------

display(gold_player_stats_v2)

# COMMAND ----------

silver_games_schedule_v2.columns

# COMMAND ----------

game_clean_cols = ['DAY',
 'DATE',
 'EASTERN',
 'LOCAL',
 'AWAY',
 'HOME',
 'season',
 'team',
 'playerTeam',
 'home_or_away',
 'gameDate',
 'opposingTeam',
 'gameId',
 'game_Total_shotsOnGoalFor',
 'game_Total_missedShotsFor',
 'game_Total_blockedShotAttemptsFor',
 'game_Total_shotAttemptsFor',
 'game_Total_goalsFor',
 'game_Total_penaltiesFor',
 'game_Total_faceOffsWonFor',
 'game_Total_hitsFor',
 'game_Total_goalsAgainst',
 'game_Total_penaltiesAgainst',
 'game_Total_faceOffsWonAgainst',
 'game_Total_hitsAgainst',
 ]

# COMMAND ----------

clean_cols = ['team',
 'season',
 'home_or_away',
 'gameDate',
 'playerTeam',
 'opposingTeam',
 'gameId',
 'playerId',
 'shooterName',
 'position',
 'player_Total_icetime',
 'player_Total_shifts',
 'player_Total_iceTimeRank',
 'player_Total_primaryAssists',
 'player_Total_secondaryAssists',
 'player_Total_shotsOnGoal',
 'player_Total_missedShots',
 'player_Total_blockedShotAttempts',
 'player_Total_shotAttempts',
 'player_Total_points',
 'player_Total_goals',
 'player_Total_rebounds',
 'player_Total_reboundGoals',
 'player_Total_savedShotsOnGoal',
 'player_Total_savedUnblockedShotAttempts',
 'player_Total_hits',
 'player_Total_takeaways',
 'player_Total_giveaways',
 'player_Total_lowDangerShots',
 'player_Total_mediumDangerShots',
 'player_Total_highDangerShots',
 'player_Total_lowDangerGoals',
 'player_Total_mediumDangerGoals',
 'player_Total_highDangerGoals',
 'player_Total_unblockedShotAttempts',
 'player_Total_OnIce_F_shotsOnGoal',
 'player_Total_OnIce_F_missedShots',
 'player_Total_OnIce_F_blockedShotAttempts',
 'player_Total_OnIce_F_shotAttempts',
 'player_Total_OnIce_F_goals',
 'player_Total_OnIce_F_lowDangerShots',
 'player_Total_OnIce_F_mediumDangerShots',
 'player_Total_OnIce_F_highDangerShots',
 'player_Total_OnIce_F_lowDangerGoals',
 'player_Total_OnIce_F_mediumDangerGoals',
 'player_Total_OnIce_F_highDangerGoals',
 'player_Total_OnIce_A_shotsOnGoal',
 'player_Total_OnIce_A_shotAttempts',
 'player_Total_OnIce_A_goals',
 'player_Total_OffIce_F_shotAttempts',
 'player_Total_OffIce_A_shotAttempts',
 'player_PP_icetime',
 'player_PP_shifts',
 'player_PP_iceTimeRank',
 'player_PP_primaryAssists',
 'player_PP_secondaryAssists',
 'player_PP_shotsOnGoal',
 'player_PP_missedShots',
 'player_PP_blockedShotAttempts',
 'player_PP_shotAttempts',
 'player_PP_points',
 'player_PP_goals',
 'player_PP_rebounds',
 'player_PP_reboundGoals',
 'player_PP_savedShotsOnGoal',
 'player_PP_hits',
 'player_PP_takeaways',
 'player_PP_giveaways',
 'player_PK_icetime',
 'player_PK_shifts',
 'player_PK_iceTimeRank',
 'player_PK_primaryAssists',
 'player_PK_secondaryAssists',
 'player_PK_shotsOnGoal',
 'player_PK_missedShots',
 'player_PK_blockedShotAttempts',
 'player_PK_shotAttempts',
 'player_PK_points',
 'player_PK_goals',
 'player_PK_rebounds',
 'player_PK_reboundGoals',
 'player_PK_savedShotsOnGoal',
 'player_PK_savedUnblockedShotAttempts',
 'player_PK_hits',
 'player_PK_takeaways',
 'player_PK_giveaways',
 'player_EV_icetime',
 'player_EV_shifts',
 'player_EV_iceTimeRank',
 'player_EV_primaryAssists',
 'player_EV_secondaryAssists',
 'player_EV_shotsOnGoal',
 'player_EV_missedShots',
 'player_EV_blockedShotAttempts',
 'player_EV_shotAttempts',
 'player_EV_points',
 'player_EV_goals',
 'player_EV_rebounds',
 'player_EV_reboundGoals',
 'player_EV_savedShotsOnGoal',
 'player_EV_savedUnblockedShotAttempts',
 'player_EV_hits',
 'player_EV_takeaways',
 'player_EV_giveaways',
 'playerGamesPlayedRolling',
 'playerMatchupPlayedRolling',
 'previous_player_Total_icetime',
 'matchup_previous_player_Total_icetime',
 'previous_player_Total_shifts',
 'matchup_previous_player_Total_shifts',
 'previous_player_Total_iceTimeRank',
 'matchup_previous_player_Total_iceTimeRank',
 'previous_player_Total_primaryAssists',
 'matchup_previous_player_Total_primaryAssists',
 'previous_player_Total_secondaryAssists',
 'matchup_previous_player_Total_secondaryAssists',
 'previous_player_Total_shotsOnGoal',
 'matchup_previous_player_Total_shotsOnGoal',
 'previous_player_Total_missedShots',
 'matchup_previous_player_Total_missedShots',
 'previous_player_Total_blockedShotAttempts',
 'matchup_previous_player_Total_blockedShotAttempts',
 'previous_player_Total_shotAttempts',
 'matchup_previous_player_Total_shotAttempts',
 'previous_player_Total_points',
 'matchup_previous_player_Total_points',
 'previous_player_Total_goals',
 'matchup_previous_player_Total_goals',
 'previous_player_Total_rebounds',
 'matchup_previous_player_Total_rebounds',
 'previous_player_Total_reboundGoals',
 'matchup_previous_player_Total_reboundGoals',
 'previous_player_Total_savedShotsOnGoal',
 'matchup_previous_player_Total_savedShotsOnGoal',
 'previous_player_Total_savedUnblockedShotAttempts',
 'matchup_previous_player_Total_savedUnblockedShotAttempts',
 'previous_player_Total_hits',
 'matchup_previous_player_Total_hits',
 'previous_player_Total_takeaways',
 'matchup_previous_player_Total_takeaways',
 'previous_player_Total_giveaways',
 'matchup_previous_player_Total_giveaways',
 'previous_player_Total_lowDangerShots',
 'matchup_previous_player_Total_lowDangerShots',
 'previous_player_Total_mediumDangerShots',
 'matchup_previous_player_Total_mediumDangerShots',
 'previous_player_Total_highDangerShots',
 'matchup_previous_player_Total_highDangerShots',
 'previous_player_Total_lowDangerGoals',
 'matchup_previous_player_Total_lowDangerGoals',
 'previous_player_Total_mediumDangerGoals',
 'matchup_previous_player_Total_mediumDangerGoals',
 'previous_player_Total_highDangerGoals',
 'matchup_previous_player_Total_highDangerGoals',
 'previous_player_Total_unblockedShotAttempts',
 'matchup_previous_player_Total_unblockedShotAttempts',
 'previous_player_Total_OnIce_F_shotsOnGoal',
 'matchup_previous_player_Total_OnIce_F_shotsOnGoal',
 'previous_player_Total_OnIce_F_missedShots',
 'previous_player_PP_blockedShotAttempts',
 'matchup_previous_player_PP_blockedShotAttempts',
 'previous_player_PP_shotAttempts',
 'matchup_previous_player_PP_shotAttempts',
 'previous_player_PP_points',
 'previous_player_PP_goals',
 'average_player_PP_goals_last_3_games',
 'average_player_PP_goals_last_7_games',
 'matchup_previous_player_PP_goals',
 'previous_player_PP_savedShotsOnGoal',
 'matchup_previous_player_PP_savedShotsOnGoal',
 'previous_player_PP_hits',
 'matchup_previous_player_PP_hits',
]

# COMMAND ----------

games_clean = silver_games_schedule_v2[[game_clean_cols]]
games_clean = games_clean.withColumn("isWin", when(col("game_Total_goalsFor") > col("game_Total_goalsAgainst"), "Yes").otherwise("No")).filter(col("gameId").isNotNull())
display(games_clean)

# COMMAND ----------

gold_player_stats_clean = gold_player_stats_v2[[clean_cols]]

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_player_stats_clean")
gold_player_stats_clean.write.format("delta").mode("overwrite").saveAsTable("lr_nhl_demo.dev.gold_player_stats_clean")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_game_stats_clean")
games_clean.write.format("delta").mode("overwrite").saveAsTable("lr_nhl_demo.dev.gold_game_stats_clean")
