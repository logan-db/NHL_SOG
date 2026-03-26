# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import col, expr, desc

# COMMAND ----------

# DBTITLE 1,Catalog Setup
# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

# DBTITLE 1,Define Latest Games Dataset
# NHL uses 8-digit season format: 20252026 = 2025-26 season
CURRENT_SEASON = 20252026

latest_games = (
    spark.table("dev.clean_prediction_summary")
    .filter(
        (col("gameId").isNull())
        & (col("is_last_played_game") == True)
        & (col("season") == CURRENT_SEASON)
        # & (col("shooterName") == "Alex Ovechkin")
    )
    .orderBy(desc("predictedSOG"), "gameDate")
    .limit(100)
)

# COMMAND ----------

# DBTITLE 1,Parse metadata
rows = latest_games.limit(1).collect()
if not rows:
    raise ValueError(
        "No rows in latest_games. Check clean_prediction_summary has upcoming games "
        f"for season={CURRENT_SEASON} (NHL 8-digit format)."
    )
first_row = rows[0]

# Initialize an empty dictionary to store column names and values
column_value_dict = {}

# Iterate through each column in the DataFrame
for column in latest_games.columns:
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
    for col in latest_games.schema.fields
    if "comment" in col.metadata
}

modified_comments = (
    str(column_comments).replace("{", "").replace("}", "").replace("'", "`")
)

input_column_names = []

for column in latest_games.columns:
    if "%" in column:
        input_column_names.append(f"`{column}`")
    else:
        input_column_names.append(column)

input_column_names_clean = (
    str(input_column_names).replace("[", "").replace("]", "").replace("'", "")
)
print(input_column_names_clean)

# COMMAND ----------

# DBTITLE 1,Fetch injury/trade context for enhanced explanations
injury_context = ""
trade_context = ""
accuracy_context = ""

try:
    _avail = spark.table("dev.bronze_player_availability")
    _unavail = _avail.filter(col("status").isin("UNAVAILABLE", "IR", "LTIR", "OUT", "DAY_TO_DAY"))
    if _unavail.count() > 0:
        _unavail_list = _unavail.select("playerName", "team", "status", "injury_type").limit(30).collect()
        injury_lines = [f"  - {r['playerName']} ({r['team']}): {r['status']}" + (f" ({r['injury_type']})" if r['injury_type'] else "") for r in _unavail_list]
        injury_context = "\n\nCurrently injured/unavailable players (excluded from predictions):\n" + "\n".join(injury_lines)
except Exception:
    pass

try:
    _txn = spark.table("dev.bronze_player_transactions")
    from pyspark.sql.functions import max as spark_max
    _recent = _txn.filter(col("transaction_type") == "TRADE").orderBy(desc("transaction_date")).limit(10).collect()
    if _recent:
        trade_lines = [f"  - {r['playerName']}: {r['from_team']} -> {r['to_team']} ({r['transaction_date']})" for r in _recent]
        trade_context = "\n\nRecent trades (predictions use new team context):\n" + "\n".join(trade_lines)
except Exception:
    pass

try:
    _acc = spark.table("dev.prediction_accuracy").orderBy(desc("prediction_date")).limit(1).collect()
    if _acc:
        accuracy_context = f"\n\nModel recent accuracy: 7-day rolling MAE = {_acc[0]['rolling_mae_7d']}, 30-day rolling MAE = {_acc[0]['rolling_mae_30d']}"
except Exception:
    pass

# COMMAND ----------

# DBTITLE 1,Setup Prompt and Call AI_Query()
endpoint_name = "databricks-claude-3-7-sonnet"

prompt = f"""
You are an NHL analytics expert. Analyze this player's upcoming game and provide a well-formatted 2-4 paragraph analysis. Cover:

1. **Player profile**: Their role (e.g., major shooter, powerplay specialist), recent form, shot consistency. Note if they take most shots on powerplay (playerLast7PPSOG% high) or even strength. If the player recently returned from injury or was traded, mention that context and how it may affect their shot volume.

2. **Key metrics**: Predicted SOG (predictedSOG), average SOG (playerAvgSOGLast7, playerAvgSOGLast3), last game SOG (playerLastSOG). How often they hit 2+ and 3+ SOG this season and vs this opponent. Whether they consistently shoot (check stddev/variance if available).

3. **Matchup**: How the opponent ranks in SOG allowed (oppSOGAgainstRank%), penalties (oppPenaltiesRank%), PK (oppPKSOGRank%). If opponent allows lots of penalties and the player is a PP specialist, highlight that advantage. Consider opponent goalie quality if available.

4. **Recommendation**: Whether they are a strong SOG pick based on the data. Be specific with numbers.
{injury_context}{trade_context}{accuracy_context}

Schema/column meanings for reference:
{modified_comments}

Keep the analysis concise (2-4 short paragraphs). Use specific stats from the input. Format with clear structure.

Input Row Data:

"""

# Use ai_query for batch inference
# Escape single quotes in prompt for SQL string literal (e.g. "player's" -> "player''s")
prompt_escaped = prompt.replace("'", "''")
ai_query_expr = f"""
ai_query('{endpoint_name}', 
    request => '{prompt_escaped}' || STRING(struct(*)),
    returnType => 'STRING'
    ) AS Explanation
"""

df_out = latest_games.selectExpr("*", ai_query_expr)

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
    "overwriteSchema", "true"
).option("delta.enableChangeDataFeed", "true").saveAsTable(
    "lr_nhl_demo.dev.llm_summary"
)
# Ensure CDF for Lakebase TRIGGERED sync (option may not apply on saveAsTable)
spark.sql(
    "ALTER TABLE lr_nhl_demo.dev.llm_summary SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)
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

display(
    spark.table("dev.clean_prediction_v2")
    .filter(col("gameId").isNull())
    .select("gameId", "playerId", "gameDate", "shooterName", "predictedSOG")
    .orderBy("gameDate", desc("predictedSOG"))
)

# COMMAND ----------

test_out = df_out.orderBy("gameDate", desc("predictedSOG")).limit(1)
display(test_out)

test_base = (
    spark.table("dev.clean_prediction_v2")
    .filter(col("gameId").isNull())
    .select("gameId", "playerId", "gameDate", "shooterName", "predictedSOG")
    .orderBy("gameDate", desc("predictedSOG"))
    .limit(1)
)
display(test_base)

display(
    test_base.join(
        test_out.select("gameId", "playerId", "gameDate", "Explanation"),
        how="left",
        on=["playerId", "gameDate"],
    )
)

# COMMAND ----------

column_comments = {
    col.name: col.metadata.get("comment", None)
    for col in latest_games.schema.fields
    if "comment" in col.metadata
}
column_comments

# COMMAND ----------


def process_data_and_generate_prompt(
    table_name="lr_nhl_demo.dev.clean_prediction_summary",
    # player_name="Auston Matthews",
    num_games=100,
):
    # Read the table
    df = spark.table(table_name)

    # Get column comments
    column_comments = {
        col.name: col.metadata.get("comment", None)
        for col in df.schema.fields
        if "comment" in col.metadata
    }

    # Select relevant columns and get the latest games
    latest_games = (
        df
        # .filter(col("shooterName") == player_name)
        .orderBy(desc("gameDate"), desc("predictedSOG")).limit(num_games)
    )

    # Convert to a pandas DataFrame for easier handling
    games_data = latest_games.toPandas().to_dict("records")

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
column_comments = {
    col.name: col.metadata.get("comment", None)
    for col in df.schema.fields
    if "comment" in col.metadata
}

# Select relevant columns and get the latest games
latest_games = df.orderBy(desc("gameDate"), desc("predictedSOG")).limit(num_games)

# Convert to a pandas DataFrame for easier handling
games_data = latest_games.toPandas().to_dict("records")

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
# MAGIC       oppGoalsAgainstRank%: Opponent Team percentage rank of total goals against. Lower = opponent allows more goals = better.
# MAGIC       oppSOGAgainstRank%: Opponent Team percentage rank of total shots on goal against. Lower = opponent allows more shots = better.
# MAGIC       oppPenaltiesRank%: Opponent Team percentage rank of total penalties for. Higher = opponent takes more penalties = better.
# MAGIC       oppPKSOGRank%: Opponent Team percentage rank of penalty kill shots on goal against per penalty. Lower = opponent allows more PK SOG = better.
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
games_data = latest_games.toPandas().to_dict("records")

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
    model="databricks-claude-3-7-sonnet", prompt=prompt, max_tokens=300
)

print(response.text)
