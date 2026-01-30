# Databricks notebook source
# MAGIC %md
# MAGIC ## NHL City Abbreviation Function

# COMMAND ----------

from nhl_team_city_to_abbreviation import nhl_team_city_to_abbreviation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lr_nhl_demo.dev.city_to_abbreviation(city_name STRING)
# MAGIC   RETURNS STRING
# MAGIC   LANGUAGE PYTHON
# MAGIC   AS $$
# MAGIC     from nhl_team_city_to_abbreviation import nhl_team_city_to_abbreviation
# MAGIC
# MAGIC     return nhl_team_city_to_abbreviation.get(city_name, "Unknown")
# MAGIC   $$

# COMMAND ----------

# MAGIC %md
# MAGIC ## Player Prediction Analysis Function

# COMMAND ----------

# DBTITLE 1,Prod
from pyspark.sql.functions import col, expr

input_dataframe = spark.table("lr_nhl_demo.dev.clean_prediction_summary").filter(
    (col("gameId").isNull())
    & (col("is_last_played_game_team") == 1)
    & (col("season") == 2025)
    # & (col("shooterName") == "Alex Ovechkin")
)

# Initialize an empty dictionary to store column names and values
column_value_dict = {}

# Get the first row of the DataFrame
first_row = input_dataframe.limit(1).collect()[0]

# Iterate through each column in the DataFrame
for column in input_dataframe.columns:
    if "%" in column:
        # If the column name contains '%', wrap it in backticks
        column_name = f"`{column}`"
    else:
        column_name = column

    # Get the value for this column from the first row
    value = first_row[column]

    # Add the column name and value to the dictionary
    column_value_dict[column_name] = value

# Print the resulting dictionary
cleaned_col_vals = (
    str(column_value_dict).replace("{", "").replace("}", "").replace("'", "")
)
print(cleaned_col_vals)

column_comments = {
    col.name: col.metadata.get("comment", None)
    for col in input_dataframe.schema.fields
    if "comment" in col.metadata
}

modified_comments = (
    str(column_comments).replace("{", "").replace("}", "").replace("'", "`")
)

input_column_names = []

for column in input_dataframe.columns:
    if "%" in column:
        input_column_names.append(f"`{column}`")
    else:
        input_column_names.append(column)

input_column_names_clean = (
    str(input_column_names).replace("[", "").replace("]", "").replace("'", "")
)

endpoint_name = "databricks-claude-3-7-sonnet"

prompt = f"""
You are provided with a row of NHL statistics for a given player. The goal is to explain and analyze the players next games shots on goal (predictedSOG). The input is a row of NHL statistics of a players previous games along with the players predicted shots on goal for the next game (predictedSOG). Use the input row and provide analysis on why the model predicted this predictedSOG value. 

When analyzing the stats, use the following schema comments to understand what each column/statistic means in order to better explain: 

{modified_comments}
      

Print out the following Input Row Data that you are using in your analysis.

Input Row Data:
{cleaned_col_vals}

"""
# State the predictedSOG value along with other statistics during your Analysis.

# Use ai_query for batch inference
ai_query_expr = f"""
ai_query('{endpoint_name}', 
CONCAT('{prompt}')) as Explanation
"""

df_out = input_dataframe.selectExpr("*", ai_query_expr)

# Write to the output table
display(df_out)

# COMMAND ----------

# DBTITLE 1,Dev
from pyspark.sql.functions import col, expr, desc

input_dataframe = (
    spark.table("lr_nhl_demo.dev.clean_prediction_summary")
    .filter(
        (col("gameId").isNull())
        & (col("is_last_played_game_team") == 1)
        & (col("season") == 5)
        # & (col("shooterName") == "Alex Ovechkin")
    )
    .orderBy(desc("predictedSOG"), "gameDate")
    .limit(3)
)

# Initialize an empty dictionary to store column names and values
column_value_dict = {}

# Get the first row of the DataFrame
first_row = input_dataframe.limit(1).collect()[0]

# Iterate through each column in the DataFrame
for column in input_dataframe.columns:
    if "%" in column:
        # If the column name contains '%', wrap it in backticks
        column_name = f"`{column}`"
    else:
        column_name = column

    # Get the value for this column from the first row
    value = first_row[column]

    # Add the column name and value to the dictionary
    column_value_dict[column_name] = value

# Print the resulting dictionary
cleaned_col_vals = (
    str(column_value_dict).replace("{", "").replace("}", "").replace("'", "")
)
print(cleaned_col_vals)

column_comments = {
    col.name: col.metadata.get("comment", None)
    for col in input_dataframe.schema.fields
    if "comment" in col.metadata
}

modified_comments = (
    str(column_comments).replace("{", "").replace("}", "").replace("'", "`")
)

input_column_names = []

for column in input_dataframe.columns:
    if "%" in column:
        input_column_names.append(f"`{column}`")
    else:
        input_column_names.append(column)

input_column_names_clean = (
    str(input_column_names).replace("[", "").replace("]", "").replace("'", "")
)
print(input_column_names_clean)

endpoint_name = "databricks-claude-3-7-sonnet"

prompt = f"""
You are provided with a row of NHL statistics for a given player. The goal is to explain and analyze the players next games shots on goal (predictedSOG). The input is a row of NHL statistics of a players previous games along with the players predicted shots on goal for the next game (predictedSOG). Use the input row and provide analysis on why the model predicted this predictedSOG value. 

When analyzing the stats, use the following schema comments to understand what each column/statistic means in order to better explain: 

{modified_comments}
      
State the predictedSOG value along with other statistics during your Analysis.

Input Row Data:

"""

# Use ai_query for batch inference
ai_query_expr = f"""
ai_query('{endpoint_name}', 
    request => '{prompt}' || STRING(struct(*)),
    returnType => 'STRING'
    ) AS Explanation
"""

df_out = input_dataframe.selectExpr("*", ai_query_expr)

# Write to the output table
display(df_out)

# COMMAND ----------

column_comments = {
    col.name: col.metadata.get("comment", None)
    for col in spark.table("lr_nhl_demo.dev.clean_prediction_summary").schema.fields
    if "comment" in col.metadata
}

modified_comments = (
    str(column_comments).replace("{", "").replace("}", "").replace("'", "`")
)
print(modified_comments)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE lr_nhl_demo.dev.clean_prediction_summary;
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, expr

input_dataframe = spark.table("lr_nhl_demo.dev.clean_prediction_summary").filter(
    (col("gameId").isNull())
    & (col("is_last_played_game_team") == 1)
    & (col("season") == 2025)
    & (col("shooterName") == "Alex Ovechkin")
)

input_column_names = []

for column in input_dataframe.columns:
    if "%" in column:
        input_column_names.append(f"`{column}`")
    else:
        input_column_names.append(column)

input_column_names_clean = (
    str(input_column_names).replace("[", "").replace("]", "").replace("'", "")
)
input_column_names_clean

# COMMAND ----------

column_comments = {
    col.name: col.metadata.get("comment", None)
    for col in input_dataframe.schema.fields
    if "comment" in col.metadata
}

modified_comments = (
    str(column_comments).replace("{", "").replace("}", "").replace("'", "`")
)

input_column_names = []

for column in input_dataframe.columns:
    if "%" in column:
        input_column_names.append(f"`{column}`")
    else:
        input_column_names.append(column)

input_column_names_clean = (
    str(input_column_names).replace("[", "").replace("]", "").replace("'", "")
)

endpoint_name = "databricks-claude-3-7-sonnet"

prompt = f"""
You are provided with a row of NHL statistics for a given player. The goal is to explain and analyze the players next games shots on goal (predictedSOG). The input is a row of NHL statistics of a players previous games along with the players predicted shots on goal for the next game (predictedSOG). Use the input row and provide analysis on why the model predicted this predictedSOG value. 

When analyzing the stats, use the following schema comments to understand what each column/statistic means in order to better explain: 

{modified_comments}
      

Print out the following Input Row Data that you are using in your analysis.

Input Row Data:
{cleaned_col_vals}

"""
# State the predictedSOG value along with other statistics during your Analysis.

# Use ai_query for batch inference
ai_query_expr = f"""
ai_query('{endpoint_name}', 
CONCAT('{prompt}')) as Explanation
"""

df_out = input_dataframe.selectExpr("*", ai_query_expr)

# Write to the output table
display(df_out)
# .write.mode("overwrite").option("mergeSchema", "true").saveAsTable(output_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   *,
# MAGIC   ai_query(
# MAGIC     'databricks-claude-3-7-sonnet',
# MAGIC     CONCAT(
# MAGIC       'You are provided with a row of NHL statistics for a given player. The goal is to explain and analyze the player\'s next game\'s shots on goal (predictedSOG). The input is a row of NHL statistics of a player\'s previous games along with the player\'s predicted shots on goal for the next game (predictedSOG). Please use the input row and provide analysis on why the model predicted this predictedSOG value.
# MAGIC
# MAGIC       When analyzing the stats, use the following to understand what each column/statistic means: ', column_comments'
# MAGIC
# MAGIC       Input Row: ', shooterName, predictedSOG, playerLastSOG, playerAvgSOGLast7, matchup_average_player_Total_shotsOnGoal_last_7_games)
# MAGIC   ) AS Explanation
# MAGIC FROM lr_nhl_demo.dev.clean_prediction_summary
# MAGIC WHERE gameId IS NULL AND season = 2024 AND is_last_played_game_team = 1 AND shooterName = "Alex Ovechkin";

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM lr_nhl_demo.dev.clean_prediction_summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## Latest Stats Functions

# COMMAND ----------

df = spark.table("lr_nhl_demo.dev.bronze_schedule_2023_v2")

# COMMAND ----------

from pyspark.sql.functions import *

df_test = df.withColumn(
    "state_abbr", expr("lr_nhl_demo.dev.city_to_abbreviation(AWAY)")
)
display(df_test)

# COMMAND ----------

shooter_name = "Alex Ovechkin"
n_games = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC       gameDate,
# MAGIC       playerTeam,
# MAGIC       opposingTeam,
# MAGIC       shooterName,
# MAGIC       home_or_away,
# MAGIC       season,
# MAGIC       player_Total_shotsOnGoal,
# MAGIC       player_Total_hits,
# MAGIC       player_Total_goals,
# MAGIC       player_Total_points,
# MAGIC       player_Total_shotAttempts,
# MAGIC       player_Total_shotsOnGoal,
# MAGIC       player_Total_primaryAssists,
# MAGIC       player_Total_secondaryAssists,
# MAGIC       player_Total_iceTimeRank
# MAGIC     FROM
# MAGIC       lr_nhl_demo.dev.gold_player_stats_v2
# MAGIC     WHERE
# MAGIC       shooterName = "Alex Ovechkin"
# MAGIC       AND gameId IS NOT NULL
# MAGIC     ORDER BY
# MAGIC       gameDate DESC
# MAGIC     LIMIT 3

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lr_nhl_demo.dev.get_latest_stats_sql(shooter_name STRING DEFAULT 'Alex Ovechkin' COMMENT 'The name of the player for whom the stats are to be retrieved. Defaults to "Alex Ovechkin"', n_games INTEGER DEFAULT 3 COMMENT 'The number of latest games to retrieve stats for. Defaults to 3.')
# MAGIC RETURNS TABLE(gameDate DATE, playerTeam STRING, opposingTeam STRING, shooterName STRING, home_or_away STRING, season INTEGER, player_Total_shotsOnGoal INTEGER, player_Total_hits INTEGER, player_Total_goals INTEGER, player_Total_points INTEGER, player_Total_shotAttempts INTEGER, player_Total_primaryAssists INTEGER, player_Total_secondaryAssists INTEGER, player_Total_iceTimeRank INTEGER)
# MAGIC COMMENT 'This function retrieves the latest statistics for a specified player over a specified number of games.'
# MAGIC RETURN (
# MAGIC   WITH RankedGames AS (
# MAGIC     -- Create a temporary table with a row number based on the most recent games first
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (ORDER BY gameDate DESC) AS rn
# MAGIC     FROM lr_nhl_demo.dev.gold_player_stats_v2
# MAGIC     WHERE shooterName = get_latest_stats_sql.shooter_name
# MAGIC       AND gameId IS NOT NULL
# MAGIC   )
# MAGIC   SELECT
# MAGIC     gameDate,
# MAGIC     playerTeam,
# MAGIC     opposingTeam,
# MAGIC     shooterName,
# MAGIC     home_or_away,
# MAGIC     season,
# MAGIC     player_Total_shotsOnGoal,
# MAGIC     player_Total_hits,
# MAGIC     player_Total_goals,
# MAGIC     player_Total_points,
# MAGIC     player_Total_shotAttempts,
# MAGIC     player_Total_primaryAssists,
# MAGIC     player_Total_secondaryAssists,
# MAGIC     player_Total_iceTimeRank
# MAGIC   FROM RankedGames
# MAGIC   -- Filter to only include the top n_games as specified by the function's parameter
# MAGIC   WHERE rn <= get_latest_stats_sql.n_games
# MAGIC )

# COMMAND ----------
