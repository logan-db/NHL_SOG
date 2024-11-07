# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import col, expr, desc

# COMMAND ----------

# DBTITLE 1,Catalog Setup
# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

# DBTITLE 1,Define Latest Games Dataset
latest_games = (
  spark.table("dev.clean_prediction_summary")
    .filter(
    (col("gameId").isNull())
    & (col("is_last_played_game") == True)
    & (col("season") == 2024)
    # & (col("shooterName") == "Alex Ovechkin")
          )
    .orderBy(desc("predictedSOG"), "gameDate")
    .limit(100)
)

# COMMAND ----------

# DBTITLE 1,Parse metadata
# Initialize an empty dictionary to store column names and values
column_value_dict = {}

# Get the first row of the DataFrame
first_row = latest_games.limit(1).collect()[0]

# Iterate through each column in the DataFrame
for column in latest_games.columns:
    if '%' in column:
        # If the column name contains '%', wrap it in backticks
        column_name = f"`{column}`"
    else:
        column_name = column
    
    # Get the value for this column from the first row
    value = first_row[column]
    
    # Add the column name and value to the dictionary
    column_value_dict[column_name] = value

# Print the resulting dictionary
cleaned_col_vals = str(column_value_dict).replace("{", "").replace("}", "").replace("'", "")
print(cleaned_col_vals)

column_comments = {
    col.name: col.metadata.get("comment", None)
    for col in latest_games.schema.fields
    if "comment" in col.metadata
}

modified_comments = str(column_comments).replace("{", "").replace("}", "").replace("'", "`")

input_column_names = []

for column in latest_games.columns:
    if '%' in column:
        input_column_names.append(f"`{column}`")
    else:
        input_column_names.append(column)

input_column_names_clean = str(input_column_names).replace("[", "").replace("]", "").replace("'", "")
print(input_column_names_clean)

# COMMAND ----------

# DBTITLE 1,Setup Prompt and Call AI_Query()
endpoint_name = "databricks-meta-llama-3-1-70b-instruct"

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

df_out = (
  latest_games
  .selectExpr(
    "*",
    ai_query_expr
  )
)

# COMMAND ----------

# DBTITLE 1,Join Explanations to BI table
# BI_Explanations = (
#   spark.table("dev.clean_prediction_v2")
#   .join(df_out.select("playerId", "gameDate", "Explanation"), how='left', on=["playerId", "gameDate"])
#   .orderBy(desc("predictedSOG"), "gameDate")
# )
# display(BI_Explanations)

# COMMAND ----------

# DBTITLE 1,Save Dataframe to UC
df_out.write.format("delta").mode("overwrite").option(
    "mergeSchema", "true"
).saveAsTable("lr_nhl_demo.dev.llm_summary")

print("Data written to table: lr_nhl_demo.dev.llm_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Testing Logic --> SKIPPED

# COMMAND ----------

testing = True

if testing:
  dbutils.notebook.exit("Exiting the notebook as requested.")

# COMMAND ----------

display(df_out)

# COMMAND ----------

display(spark.table("dev.clean_prediction_v2")
        .filter(col("gameId").isNull())
        .select("gameId", "playerId", "gameDate", "shooterName", "predictedSOG")
        .orderBy("gameDate", desc("predictedSOG"))
        )

# COMMAND ----------

test_out = df_out.orderBy("gameDate", desc("predictedSOG")).limit(1)
display(test_out)

test_base = (spark.table("dev.clean_prediction_v2")
        .filter(col("gameId").isNull())
        .select("gameId", "playerId", "gameDate", "shooterName", "predictedSOG")
        .orderBy("gameDate", desc("predictedSOG"))
        .limit(1)
)
display(test_base)

display(
  test_base.join(test_out.select("gameId", "playerId", "gameDate", "Explanation"), how='left', on=["playerId", "gameDate"])
)

# COMMAND ----------

column_comments = {col.name: col.metadata.get('comment', None) for col in latest_games.schema.fields if 'comment' in col.metadata}
column_comments

# COMMAND ----------

def process_data_and_generate_prompt(table_name="lr_nhl_demo.dev.clean_prediction_summary", 
                                     # player_name="Auston Matthews", 
                                     num_games=100,
                                     ):
    # Read the table
    df = spark.table(table_name)
    
    # Get column comments
    column_comments = {col.name: col.metadata.get('comment', None) for col in df.schema.fields if 'comment' in col.metadata}
    
    # Select relevant columns and get the latest games
    latest_games = (
      df
        # .filter(col("shooterName") == player_name)
        .orderBy(desc("gameDate"), desc('predictedSOG'))
        .limit(num_games)
        )
    
    # Convert to a pandas DataFrame for easier handling
    games_data = latest_games.toPandas().to_dict('records')
    
    # Create the prompt
    prompt = f"""
    Table data:
    {games_data}

    Column descriptions:
    {column_comments}

    Instructions:
    1. Analyze the data in the table above, the data is relevant statistics for player Shots on Goal (SOG) predictions in upcoming NHL Games.
    2. Use the column descriptions to understand what each column represents.
    3. Analyze the player's recent performance, the player team's and the Opposing Team's recent performance and rankings to breakdown reasoning for the player's predicted Shots on Goal value.
    4. Identify and provide the top 5 players with predicted Shots on Goal values that you can explain using the data.

    Question: Based on the data and column descriptions provided, give a detailed analysis of the top 5 players with predicted Shots on Goal values that you can explain using the data. How accurate do you think the predictions will be, and what trends do you observe? Include insights based on the meanings of the columns as described.
    """
    
    return prompt

# COMMAND ----------

output_prompt = process_data_and_generate_prompt()

output_prompt

# COMMAND ----------

df = spark.table("lr_nhl_demo.dev.clean_prediction_summary")

# Get column comments
column_comments = {col.name: col.metadata.get('comment', None) for col in df.schema.fields if 'comment' in col.metadata}

# Select relevant columns and get the latest games
latest_games = (
  df
    .orderBy(desc("gameDate"), desc('predictedSOG'))
    .limit(num_games)
    )

# Convert to a pandas DataFrame for easier handling
games_data = latest_games.toPandas().to_dict('records')

# COMMAND ----------

latest_games.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   FROM lr_nhl_demo.dev.clean_prediction_summary
# MAGIC   WHERE gameDate = current_date()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lr_nhl_demo.dev.get_clean_summary_data()
# MAGIC RETURNS TABLE(gameDate DATE,
# MAGIC  shooterName STRING,
# MAGIC  playerTeam STRING,
# MAGIC  opposingTeam STRING,
# MAGIC  season INTEGER,
# MAGIC  absVarianceAvgLast7SOG DOUBLE,
# MAGIC  predictedSOG DOUBLE,
# MAGIC  `playerLast7PPSOG%` DOUBLE,
# MAGIC  `playerLast7EVSOG%` DOUBLE,
# MAGIC  playerLastSOG DOUBLE,
# MAGIC  playerAvgSOGLast3 DOUBLE,
# MAGIC  playerAvgSOGLast7 DOUBLE,
# MAGIC  `teamGoalsForRank%` DOUBLE,
# MAGIC  `teamSOGForRank%` DOUBLE,
# MAGIC  `teamPPSOGRank%` DOUBLE,
# MAGIC  `oppGoalsAgainstRank%` DOUBLE,
# MAGIC  `oppSOGAgainstRank%` DOUBLE,
# MAGIC  `oppPenaltiesRank%` DOUBLE,
# MAGIC  `oppPKSOGRank%` DOUBLE)
# MAGIC COMMENT 'Function 1 of 2, This function retrieves recent statistics on players in upcoming NHL games. The data retrieved should be used to analyze player predictions of predictedSOG'
# MAGIC RETURN (
# MAGIC   SELECT 
# MAGIC     *
# MAGIC   FROM lr_nhl_demo.dev.clean_prediction_summary
# MAGIC   -- WHERE gameDate = current_date()
# MAGIC   LIMIT 25
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lr_nhl_demo.dev.flatten_clean_summary_data()
# MAGIC   RETURNS STRING
# MAGIC   COMMENT 'Function 2 of 3, used for flattening output data from get_clean_summary_data() function and passing into generate_prompt() function'
# MAGIC   LANGUAGE PYTHON
# MAGIC   AS $$
# MAGIC     return latest_games.toPandas().to_dict('records')
# MAGIC   $$

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION lr_nhl_demo.dev.generate_prompt(games_data STRING)
# MAGIC   RETURNS STRING
# MAGIC   COMMENT 'Function 2 of 2, This is used last to generate the prompt for the LLM. games_data parameter is the output of the get_clean_summary_data() function please convert to JSON String before passing into this function'
# MAGIC   LANGUAGE PYTHON
# MAGIC   AS $$
# MAGIC     prompt = f"""
# MAGIC     Table data:
# MAGIC     {games_data}
# MAGIC
# MAGIC     Column descriptions:
# MAGIC       gameDate: The date of the game.
# MAGIC       shooterName: Name of the player.
# MAGIC       playerTeam: Team of the player.
# MAGIC       opposingTeam: Opposing team.
# MAGIC       season: Season year.
# MAGIC       absVarianceAvgLast7SOG: Absolute variance of average shots on goal in the last 7 games of the player against the player's predicted Shots on Goal.
# MAGIC       predictedSOG: Predicted shots on goal for the player.
# MAGIC       playerLast7PPSOG%: Player's last 7 games power play shots on goal to player total shots on goal percentage.
# MAGIC       playerLast7EVSOG%: Player's last 7 games even strength shots on goal to player total shots on goal percentage.
# MAGIC       playerLastSOG: Previous game player total shots on goal.
# MAGIC       playerAvgSOGLast3: Average player total shots on goal in the last 3 games.
# MAGIC       playerAvgSOGLast7: Average player total shots on goal in the last 7 games.
# MAGIC       teamGoalsForRank%: Player Team percentage rank of total goals for. Higher is better.
# MAGIC       teamSOGForRank%: Player Team percentage rank of total shots on goal for. Higher is better.
# MAGIC       teamPPSOGRank%: Player Team percentage rank of power play shots on goal per penalty. Higher is better.
# MAGIC       oppGoalsAgainstRank%: Opponent Team percentage rank of total goals against. Higher is better.
# MAGIC       oppSOGAgainstRank%: Opponent Team percentage rank of total shots on goal against. Higher is better.
# MAGIC       oppPenaltiesRank%: Opponent Team percentage rank of total penalties for. Higher is better.
# MAGIC       oppPKSOGRank%: Opponent Team percentage rank of penalty kill shots on goal against per penalty. Higher is better.
# MAGIC
# MAGIC     Instructions:
# MAGIC     1. Analyze the data in the table above, the data is relevant statistics for player Shots on Goal (SOG) predictions in upcoming NHL Games.
# MAGIC     2. Use the column descriptions to understand what each column represents.
# MAGIC     3. Analyze the player's recent performance, the player team's and the Opposing Team's recent performance and rankings to breakdown reasoning for the player's predicted Shots on Goal value.
# MAGIC     4. Identify and provide the top 5 players with predicted Shots on Goal values that you can explain using the data.
# MAGIC
# MAGIC     Question: Based on the data and column descriptions provided, give a detailed analysis of the top 5 players with predicted Shots on Goal values that you can explain using the data. How accurate do you think the predictions will be, and what trends do you observe? Include insights based on the meanings of the columns as described.
# MAGIC     """
# MAGIC
# MAGIC     return prompt
# MAGIC   $$

# COMMAND ----------

# Convert to a list of dictionaries for easier handling
games_data = latest_games.toPandas().to_dict('records')

# Create a prompt for the LLM
prompt = f"""
Table data:
{games_data}

Instructions:
1. Analyze the data in the table above for Auston Matthews' last 5 games.
2. Compare the predicted Shots on Goal (SOG) with the actual player SOG.
3. Identify any trends or patterns in Matthews' performance.

Question: Based on the data, provide a summary of Auston Matthews' recent performance in terms of Shots on Goal (SOG). How accurate were the predictions, and what trends do you observe?
"""

# Use Databricks' AI functions to generate a response
response = ai.generate(
    model="databricks-meta-llama-3-70b-instruct",
    prompt=prompt,
    max_tokens=300
)

print(response.text)
