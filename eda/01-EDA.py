# Databricks notebook source
# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

teams_2023 = spark.table('dev.bronze_teams_2023')
shots_2023 = spark.table('dev.bronze_shots_2023')
skaters_2023 = spark.table('dev.bronze_skaters_2023')
lines_2023 = spark.table('dev.bronze_lines_2023')
games = spark.table('dev.bronze_games_historical')

silver_skaters_enriched = spark.table('dev.silver_skaters_enriched')

# COMMAND ----------

display(
  games.filter(
    (F.col('team') == 'T.B') &
    (F.col('opposingTeam') == 'NYR') &
    (F.col('situation') == 'all') &
    (F.col('season') == '2014')
    )
  )

# COMMAND ----------

display(silver_skaters_enriched)

# COMMAND ----------

# drop name, position
games_cleaned = (
        games.filter(F.col('season')=='2023')
        .drop("name", "position")
        )

orginal_count = silver_skaters_enriched.count()

skaters_team_game = silver_skaters_enriched.join(
  games_cleaned, ["team", "situation", "season"], how="inner"
    )

# assert orginal_count == skaters_team_game.count(), 
print(f"orginal_count: {orginal_count} does NOT equal {skaters_team_game.count()}")

# COMMAND ----------

# see possible 'event' values - we want this to be SHOT, MISS, GOAL
shots_2023.select('event').distinct().show()

# COMMAND ----------

shots_2023.columns

# COMMAND ----------

['shotID',
 'awaySkatersOnIce',
 'awayTeamCode',
 'event',
 'game_id',
 'goal',
 'goalieIdForShot',
 'goalieNameForShot',
 'homeSkatersOnIce',
 'homeTeamCode',
 'isPlayoffGame',
 'lastEventTeam',
 'location',
 'playerNumThatDidEvent',
 'playerNumThatDidLastEvent',
 'playerPositionThatDidEvent',
 'season',
 'shooterName',
 'shooterPlayerId',
 'shooterTimeOnIce',
 'shooterTimeOnIceSinceFaceoff',
 'shotDistance',
 'shotOnEmptyNet',
 'shotRebound',
 'shotRush',
 'shotType',
 'shotWasOnGoal',
 'speedFromLastEvent',
 'team',
 'teamCode',
 'time',
 'timeDifferenceSinceChange',
 'timeSinceFaceoff',
 'timeSinceLastEvent',
 'timeUntilNextEvent',
 'xCord',
 'xCordAdjusted',
 'xFroze',
 'xGoal',
 'xPlayContinuedInZone',
 'xPlayContinuedOutsideZone',
 'xPlayStopped',
 'xRebound',
 'xShotWasOnGoal',
 'yCord',
 'yCordAdjusted']

# COMMAND ----------

display(shots_2023)

# Aggregate Shot Data to the Game level/Player Level/Season Level, then join back to the skaters_team_game dataset
# create situation column - 'all' for now, then create columns for powerplay time and shots
# Join columns [gameId, playerId, teamCode, situation]

# COMMAND ----------

display(skaters_team_game)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Based on:
# MAGIC - PlayerID past performance (shots, icetime, powerplay, goals, assists, shot attempts, etc. over last 3,5,7,14,20, 30 games)
# MAGIC - PlayerID past performance against opposing team, home/away
# MAGIC - Team past performance 
# MAGIC - Team past performance against opposing team, home/away
# MAGIC
# MAGIC | playerId    | season | team | gameId | AGGREGATED_PLAYER_STATS | AGGREGATED_TEAM_STATS |
# MAGIC | ----------- | ------ | ---- | ------ | ----------------------- | --------------------- |

# COMMAND ----------

# Based on the player past performance, last time playing that team, 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED dev.silver_skaters_enriched;

# COMMAND ----------

display(silver_skaters_enriched)

# COMMAND ----------

display(skaters_2023)
display(lines_2023)

# COMMAND ----------

display(teams_2023)
display(shots_2023)

# COMMAND ----------

# Checking if column 'team0' ever does not equal name or team3
display(teams_2023.filter(teams_2023.team0 != teams_2023.team3))

# COMMAND ----------

display(silver_skaters_enriched)
