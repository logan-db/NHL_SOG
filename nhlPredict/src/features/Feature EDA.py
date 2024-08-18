# Databricks notebook source
# Imports
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG lr_nhl_demo

# COMMAND ----------

gold_model_data_v2 = spark.table("dev.gold_model_stats_delta_v2")

# COMMAND ----------

display(gold_model_data_v2)

# COMMAND ----------

# dbutils.fs.refreshMounts()

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED dev.gold_model_stats_delta_v2"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE DETAIL '/mnt/data_files/'

# COMMAND ----------

# MAGIC %fs ls /mnt/data_files/

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# List the categorical columns to be one-hot encoded
categorical_columns = ["playerTeam", "opposingTeam"]

# Create a list to store the pipelines for each categorical column
pipelines = []

# Apply StringIndexer and OneHotEncoder for each categorical column
for column in categorical_columns:
    # Create a StringIndexer for the current column
    indexer = StringIndexer(inputCol=column, outputCol=f"{column}_index")

    # Create a OneHotEncoder for the current column
    onehotencoder = OneHotEncoder(inputCols=[f"{column}_index"], outputCols=[f"{column}_onehot"])

    # Create a pipeline for the current column
    pipeline = Pipeline(stages=[indexer, onehotencoder])

    # Add the pipeline to the list
    pipelines.append(pipeline)

# Create a pipeline model by chaining the individual pipelines
pipeline_model = Pipeline(stages=pipelines).fit(gold_model_data_v2)

# Apply the pipeline model to transform the DataFrame
transformed_df = pipeline_model.transform(gold_model_data_v2)

# Show the transformed DataFrame
display(transformed_df)

# COMMAND ----------

# Convert the "playerId" column to a numeric type
gold_model_data_v2 = gold_model_data_v2.withColumn("playerId", col("playerId").cast("double"))

# Create an instance of the OneHotEncoder and specify the input and output columns
encoder = OneHotEncoder(inputCols=["playerId", "playerTeam", "previous_opposingTeam"],
                        outputCols=["encoded_playerId", "encoded_playerTeam", "encoded_previousOpposingTeam"])

# Fit the encoder on your DataFrame to obtain the encoding model
encoder_model = encoder.fit(gold_model_data_v2)

# Transform the DataFrame to apply the encoding
encoded_df = encoder_model.transform(gold_model_data_v2)

# Show the encoded DataFrame
display(encoded_df)

# COMMAND ----------

# Get the column names and data types of the DataFrame
columns_dtypes = encoded_df.dtypes

# Filter the numeric columns
numeric_columns = [column for column, dtype in columns_dtypes if dtype in ["int", "bigint", "float", "double"]]

# Select only the numeric columns from the DataFrame
numeric_df = gold_model_data_v2.select(*numeric_columns)

# Show the resulting DataFrame with numeric columns
display(numeric_df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.ml.feature import UnivariateFeatureSelector
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler


# Assemble features into a single vector (excluding the target variable 'player_shotsOnGoal')
assembler = VectorAssembler(
    inputCols=[col for col in gold_model_data_v2.columns if col != 'player_shotsOnGoal'],
    outputCol="features"
)
vector_data = assembler.transform(gold_model_data_v2)

# Initialize UnivariateFeatureSelector
selector = UnivariateFeatureSelector(outputCol="selectedFeatures")
selector.setFeatureType("continuous").setLabelType("continuous").setSelectionThreshold(1)

# Fit the model and transform the data
model = selector.fit(vector_data)
result = model.transform(vector_data)

# Show the resulting DataFrame with selected features
result.show()

# COMMAND ----------

