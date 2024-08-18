# Databricks notebook source
# MAGIC %md
# MAGIC # Using Databricks online tables and feature serving endpoints for retrieval augmented generation (RAG)
# MAGIC
# MAGIC This notebook illustrates how to use Databricks online tables in conjunction with feature serving endpoints to power your applications with enterprise data in real-time and at production scale. 
# MAGIC
# MAGIC This notebook creates a dummy data set and uses LangChain as an orchestration layer to augment the response of a chatbot with enterprise data served in real-time from an online table.
# MAGIC

# COMMAND ----------

# MAGIC %md ## Install required packages

# COMMAND ----------

# MAGIC %pip install --force-reinstall databricks-feature-store 
# MAGIC %pip install --force-reinstall databricks_vectorsearch 
# MAGIC %pip install --force-reinstall -v langchain openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
import mlflow 
from databricks import feature_engineering
import json
import requests
import time

fe = feature_engineering.FeatureEngineeringClient()

# COMMAND ----------

# MAGIC %md ## Create and publish feature tables 
# MAGIC
# MAGIC The following cells create feature tables with dummy data.

# COMMAND ----------

# MAGIC %md ### Setup catalog and schema to use

# COMMAND ----------

# You must have `CREATE CATALOG` privileges on the catalog.
# If necessary, change the catalog and schema name here.
username = spark.sql("SELECT current_user()").first()["current_user()"]
username = username.split(".")[0]
catalog_name = username

# Fetch the username to use as the schema name.
schema_name = "rag"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"USE SCHEMA {schema_name}")

# COMMAND ----------

# MAGIC %md ### Create feature tables and function
# MAGIC The following cell creates feature tables for user preferences and hotel prices, and it creates a function to calculate total hotel price including taxes for the duration of the stay.

# COMMAND ----------

user_preference_feature_table = f"{catalog_name}.{schema_name}.user_preferences"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {user_preference_feature_table} (
    user_id VARCHAR(255),
    avg_budget DOUBLE,
    hotel_preference VARCHAR(255),
    CONSTRAINT users_pk PRIMARY KEY(user_id)
) TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")

hotel_prices_feature_table = f"{catalog_name}.{schema_name}.hotel_prices"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {hotel_prices_feature_table} (
    hotel_id VARCHAR(255),
    name STRING,
    price DOUBLE,
    tax_rate DOUBLE, 
    CONSTRAINT hotels_pk PRIMARY KEY(hotel_id)
) TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")

hotel_price_function_name = f"{catalog_name}.{schema_name}.hotel_total_price"

spark.sql(f"""
CREATE OR REPLACE FUNCTION {hotel_price_function_name}(price DOUBLE, tax_rate DOUBLE, num_days INT)
RETURNS DOUBLE
LANGUAGE PYTHON AS
$$

def hotel_total_price(price, tax_rate, num_days):
    # Calculate price with tax
    price_with_taxes = price + price * tax_rate

    # Calculate total price for the trip using context feature num_days
    total_price = price_with_taxes * num_days

    return total_price

return hotel_total_price(price, tax_rate, num_days)
$$""")

# COMMAND ----------

# MAGIC %md ### Create dummy data for feature tables

# COMMAND ----------

schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("avg_budget", DoubleType(), False),
    StructField("hotel_preference", StringType(), True),
])

# Create some dummy data
data = [("a-456", 700.0, "city"),
        ("b-591", 100.0, "beach"),
        ("c-197", 2300.0, "cottage"),
        ("d-862", 300.0, "oceanview")]

# Create a DataFrame with the dummy data
df = spark.createDataFrame(data, schema=schema)

# Create the feature table
fe.write_table(name=user_preference_feature_table, df=df)

# COMMAND ----------

schema = StructType([
    StructField("hotel_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("tax_rate", DoubleType(), True),
])

# Create some dummy data
data = [('AB123', 'Hotel Miramalfi',  340.0, 0.05),
        ('SW345',  'Hotel Marina Riviera', 442.0, 0.07),
        ('MJ564', 'Hotel Luna Convento', 566.0, 0.03),
        ('QE278',  'Hotel Residence', 450.0, 0.02)]

# Create a DataFrame with the dummy data
df = spark.createDataFrame(data, schema=schema)

# Create the feature table
fe.write_table(name=hotel_prices_feature_table, df=df)

# COMMAND ----------

# MAGIC %md ### Sync the feature tables to a Databricks online table
# MAGIC
# MAGIC Databricks online tables are managed tables to provide low-latency lookup of your data in ML serving solutions. Use the Databricks UI to create the online table. For instructions, see the Databricks online tables documentation ([AWS](https://docs.databricks.com/en/machine-learning/feature-store/online-tables.html)|[Azure](https://docs.microsoft.com/azure/databricks/machine-learning/feature-store/online-tables)).

# COMMAND ----------

# MAGIC %md ### Create a vector index for unstructured data searches
# MAGIC
# MAGIC Databricks vector search allows you to ingest and query unstructured data. 

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

vector_search_endpoint_name = "rag_endpoint"

try:
    vsc.create_endpoint(vector_search_endpoint_name)
except Exception as e:
  if "already exists" in str(e):
    pass
  else:
    raise e

vsc.list_endpoints()
vsc.list_indexes(vector_search_endpoint_name)

# COMMAND ----------

def vector_index_ready(vsc, endpoint_name, index_name, desired_state_prefix="ONLINE"):
    index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name).describe()
    status = index["status"]["detailed_state"]
    return desired_state_prefix in status


def vector_index_exists(vsc, endpoint_name, index_name):
    try:
        vsc.get_index(endpoint_name=endpoint_name, index_name=index_name).describe()
        return True
    except Exception as e:
        if "DOES_NOT_EXIST" in str(e) or "NOT_FOUND" in str(e):
            return False
        else:
            raise e

def wait_for_vector_index_ready(vsc, endpoint_name, index_name, max_wait_time=2400, desired_state_prefix="ONLINE"):
    wait_interval = 60
    max_wait_intervals = int(max_wait_time / wait_interval)
    for i in range(0, max_wait_intervals):
        time.sleep(wait_interval)
        if vector_index_ready(vsc, endpoint_name, index_name, desired_state_prefix):
            print(f"Vector search index '{index_name}' is ready.")
            return
        else:
            print(
                f"Waiting for vector search index '{index_name}' to be in ready state."
            )
    raise Exception(f"Vector index '{index_name}' is not ready after {max_wait_time} seconds.")

# COMMAND ----------

# MAGIC %md ### Calculate embedding using Databricks foundational model 

# COMMAND ----------

def calculate_embedding(text):
    embedding_endpoint_name = "databricks-bge-large-en"
    url = f"https://{mlflow.utils.databricks_utils.get_browser_hostname()}/serving-endpoints/{embedding_endpoint_name}/invocations"
    databricks_token = mlflow.utils.databricks_utils.get_databricks_host_creds().token

    headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}
        
    data = {
        "input": text
    }
    data_json = json.dumps(data, allow_nan=True)
    
    print(f"\nCalling Embedding Endpoint: {embedding_endpoint_name}\n")
    
    response = requests.request(method='POST', headers=headers, url=url, data=data_json)
    if response.status_code != 200:
        raise Exception(f'Request failed with status {response.status_code}, {response.text}')

    return response.json()['data'][0]['embedding']

# COMMAND ----------

# MAGIC %md ### Create feature table for hotel characteristics

# COMMAND ----------

hotels_table = f"{catalog_name}.{schema_name}.hotels_data"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {hotels_table} (
    hotel_id STRING,
    embedding ARRAY<DOUBLE>,
    CONSTRAINT hotel_pk PRIMARY KEY(hotel_id)
) TBLPROPERTIES (delta.enableChangeDataFeed = true);
""")

# COMMAND ----------

schema = StructType([
    StructField("hotel_id", StringType(), False),
    StructField("embedding", ArrayType(DoubleType()), False)
])

# Create some dummy embeddings
data = [('AB123', calculate_embedding("ocean view, beautiful architecture, beautiful beaches")),
        ('SW345', calculate_embedding("city views, nice restaurants")),
        ('MJ564', calculate_embedding("beach facing, private beach")),
        ('QE278', calculate_embedding("beach views, nice beaches"))]

# Create a DataFrame with the dummy data
df = spark.createDataFrame(data, schema=schema)

# Create the feature table that holds the embeddings of the hotel characteristics
fe.write_table(name=hotels_table, df=df)

# COMMAND ----------

# MAGIC %md ### Setup Vector Search Index

# COMMAND ----------

# MAGIC %md #### Create a vector search index based on the embeddings feature table

# COMMAND ----------

hotels_table_index = f"{catalog_name}.{schema_name}.hotels_index"

try:
  vsc.create_delta_sync_index(
      endpoint_name=vector_search_endpoint_name,
      index_name=hotels_table_index,
      source_table_name=hotels_table,
      pipeline_type="TRIGGERED",
      primary_key="hotel_id",
      embedding_dimension=1024, # Match your model embedding size (bge)
      embedding_vector_column="embedding"
  )
except Exception as e:
  if "already exists" in str(e):
    pass
  else:
    raise e

# COMMAND ----------

# MAGIC %md #### Wait for the vector search index to be ready

# COMMAND ----------

vector_index = vsc.get_index(endpoint_name=vector_search_endpoint_name, index_name=hotels_table_index)
vector_index.wait_until_ready(verbose=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up feature serving endpoint
# MAGIC

# COMMAND ----------

# Import necessary classes
from databricks.feature_engineering.entities.feature_lookup import FeatureLookup
from databricks.feature_engineering import FeatureEngineeringClient, FeatureFunction
from databricks.feature_engineering.entities.feature_serving_endpoint import (
    EndpointCoreConfig,
    ServedEntity
)

# Create a feature store client
fe = FeatureEngineeringClient()

# Set endpoint and feature table names
user_endpoint_name = f"{username}-user-preferences"

# Create a lookup to fetch features by key
features=[
  FeatureLookup(
    table_name=user_preference_feature_table,
    lookup_key="user_id"
  )
]

# Create feature spec with the lookup for features
user_spec_name = f"{catalog_name}.{schema_name}.user_preferences_spec"
try:
  fe.create_feature_spec(name=user_spec_name, features=features)
except Exception as e:
  if "already exists" in str(e):
    pass
  else:
    raise e
  
# Create endpoint for serving user budget preferences
try:
  status = fe.create_feature_serving_endpoint(
    name=user_endpoint_name, 
    config=EndpointCoreConfig(
      served_entities=ServedEntity(
        feature_spec_name=user_spec_name, 
        workload_size="Small", 
        scale_to_zero_enabled=True)
      )
    )

  # Print endpoint creation status
  print(status)
except Exception as e:
  if "already exists" in str(e):
    pass
  else:
    raise e

# COMMAND ----------

hotel_endpoint_name = f"{username}-hotel-price"

# Create a lookup to fetch features by key.
hotel_features=[
  FeatureLookup(
    table_name=hotel_prices_feature_table,
    lookup_key="hotel_id",
  ),
  FeatureFunction(
    udf_name=hotel_price_function_name,
    input_bindings={
      "price": "price",
      "tax_rate": "tax_rate",
      "num_days": "num_days"
    },
    output_name="total_price"
  )
]

# Create feature spec with the lookup for features and function computation
hotel_spec_name = f"{catalog_name}.{schema_name}.hotel_price_spec"
try:
  fe.create_feature_spec(name=hotel_spec_name, features=hotel_features)
except Exception as e:
  if "already exists" in str(e):
    pass
  else:
    raise e

# Create endpoint
try: 
  status = fe.create_feature_serving_endpoint(
    name=hotel_endpoint_name, 
    config = EndpointCoreConfig(
      served_entities=ServedEntity(
        feature_spec_name=hotel_spec_name, 
        workload_size="Small", 
        scale_to_zero_enabled=True
        )
    )
  )
  print(status)
except Exception as e:
  if "already exists" in str(e):
    pass
  else:
    raise e

# COMMAND ----------

# Get endpoint status
status = fe.get_feature_serving_endpoint(name=hotel_endpoint_name)
print(status)

# COMMAND ----------

# MAGIC %md ## AI Bot powered by Databricks Feature Serving and Databricks online tables
# MAGIC 1. Automatically sync data from Delta table to online table.
# MAGIC 1. Lookup features in real-time with low latency.
# MAGIC 1. Provide context and augment chatbots with enterprise data as shown in this example.
# MAGIC 1. Implement best practices of data management in MLOps with LLMOps.

# COMMAND ----------

# MAGIC %md ### Define a tool to retreive customers and revenues
# MAGIC
# MAGIC The CustomerRetrievalTool queries the Feature Serving endpoint to serve data from the Databricks online table, thus providing context data based on the user query to the LLM.

# COMMAND ----------

from langchain.tools import BaseTool
from typing import Union


class UserPreferenceTool(BaseTool):
    name = "User Preference Feature Server"
    description = "Use this tool when you need to fetch current users travel preferences."

    def _run(self, user_id: str):
        import requests
        import pandas as pd
        import json
        import mlflow

        url = f"https://{mlflow.utils.databricks_utils.get_browser_hostname()}/serving-endpoints/{user_endpoint_name}/invocations"
        databricks_token = mlflow.utils.databricks_utils.get_databricks_host_creds().token
        
        headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}
        
        data = {
            "dataframe_records": [{"user_id": user_id}]
        }
        data_json = json.dumps(data, allow_nan=True)
        
        print(f"\nCalling Feature Serving Endpoint: {user_endpoint_name}\n")
        
        response = requests.request(method='POST', headers=headers, url=url, data=data_json)
        if response.status_code != 200:
          raise Exception(f'Request failed with status {response.status_code}, {response.text}')

        return response.json()['outputs'][0]['hotel_preference']
    
    def _arun(self, user_id: str):
        raise NotImplementedError("This tool does not support async")

# COMMAND ----------

from langchain.tools import BaseTool
from typing import Union
from typing import List
from databricks.vector_search.client import VectorSearchClient

class HotelRetrievalTool(BaseTool):
    name = "Hotels based on User's Hotel Preference Vector Server"
    description = "Use this tool when you need to fetch hotels based on users hotel_preference."

    def _run(self, hotel_preference: str):
        def calculate_embedding(text):
            embedding_endpoint_name = "databricks-bge-large-en"
            url = f"https://{mlflow.utils.databricks_utils.get_browser_hostname()}/serving-endpoints/{embedding_endpoint_name}/invocations"
            databricks_token = mlflow.utils.databricks_utils.get_databricks_host_creds().token

            headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}
                
            data = {
                "input": text
            }
            data_json = json.dumps(data, allow_nan=True)
            
            print(f"\nCalling Embedding Endpoint: {embedding_endpoint_name}\n")
            
            response = requests.request(method='POST', headers=headers, url=url, data=data_json)
            if response.status_code != 200:
                raise Exception(f'Request failed with status {response.status_code}, {response.text}')

            return response.json()['data'][0]['embedding']
            
        try:
            vsc = VectorSearchClient()
            index = vsc.get_index(endpoint_name=vector_search_endpoint_name, index_name=hotels_table_index)
            print(index)
            resp = index.similarity_search(columns=["hotel_id"], query_vector=calculate_embedding(hotel_preference), num_results=5, filters={})
            print(resp)
            data_array = resp and resp.get('result', {}).get('data_array')
            print(data_array)
        except Exception as e:
            print(f"Exception while running test case {e}")
            return []

        result = [hotel[0] for hotel in data_array]
        print(result)
        return result
    
    def _arun(self, user_id: str):
        raise NotImplementedError("This tool does not support async")

# COMMAND ----------

from langchain.tools import BaseTool
from typing import Union
from typing import List
from datetime import datetime

class TotalPriceTool(BaseTool):
    name = "Total vacation price Feature Server"
    description = "Use this tool when you need to fetch hotels total price including taxes."

    def _run(self, *args, **kwargs):
        import requests
        import pandas as pd
        import json
        import mlflow

        hotels_req = []
        hotel_ids = kwargs["hotel_ids"]
        num_days = kwargs["num_days"]

        for hotel_id in hotel_ids:
          hotels_req.append({"hotel_id": hotel_id, "num_days": num_days})
        data = {
            "dataframe_records": hotels_req
        }
        
        url = f"https://{mlflow.utils.databricks_utils.get_browser_hostname()}/serving-endpoints/{hotel_endpoint_name}/invocations"
        databricks_token = mlflow.utils.databricks_utils.get_databricks_host_creds().token
        
        headers = {'Authorization': f'Bearer {databricks_token}', 'Content-Type': 'application/json'}
        data_json = json.dumps(data, allow_nan=True)
        print("Calling Feature Serving Endpoint: " + hotel_endpoint_name)
        response = requests.request(method='POST', headers=headers, url=url, data=data_json)
        if response.status_code != 200:
          raise Exception(f'Request failed with status {response.status_code}, {response.text}')

        output_dict = []
        for output in response.json()["outputs"]:
          output_dict.append({"hotel name": output["name"], "total_price": output["total_price"]})


        return json.dumps(output_dict)
    
    def _arun(self, user_id: str):
        raise NotImplementedError("This tool does not support async")

# COMMAND ----------

# MAGIC %md ## Set up an agent that fetches enterprise data from the Databricks Lakehouse using Feature Serving 
# MAGIC The following cell uses Open API Keys. To learn how to configure secrets in Databricks secret managers, see the Databricks secret management documentation ([AWS](https://docs.databricks.com/en/security/secrets/index.html) | [Azure](https://docs.microsoft.com/azure/databricks/security/secrets/index)).

# COMMAND ----------

# Setup Open API Keys. 
# This allows the notebook to communicate with the ChatGPT conversational model.
# Alternately, you could configure your own LLM model and configure LangChain to refer to it.

OPENAI_API_KEY = dbutils.secrets.get("feature-serving", "OPENAI_API_KEY") #replace this with your openAI API key

# COMMAND ----------

from langchain.agents import initialize_agent
# Tool imports
from langchain.agents import Tool

tools = [
  UserPreferenceTool(),
  HotelRetrievalTool(),
  TotalPriceTool(),
]

import os
from langchain.chat_models import ChatOpenAI
from langchain.chains.conversation.memory import ConversationBufferWindowMemory

# Initialize LLM (this example uses ChatOpenAI because later it defines a `chat` agent)
llm = ChatOpenAI(
    openai_api_key=OPENAI_API_KEY,
    temperature=0,
    model_name='gpt-3.5-turbo'
)
# Initialize conversational memory
conversational_memory = ConversationBufferWindowMemory(
    memory_key='chat_history',
    k=5,
    return_messages=True
)
# Initialize agent with tools
aibot = initialize_agent(
    agent='chat-conversational-react-description',
    tools=tools,
    llm=llm,
    verbose=True,
    max_iterations=5,
    early_stopping_method='force',
    memory=conversational_memory
)

# COMMAND ----------

sys_msg = """Assistant is a large language model trained by OpenAI.

Assistant is designed to be plan a vacation for the input user_id.

When user ask to plan a travel, use the user_id to fetch the user hotel_preference.
Use the hotel_preference from it's trusty tools to retrieve list of hotels that fit in the user's preferences from retrival from the tools.
Use the list of hotel_ids and number of days to calculate total price and return name and total price of hotels as output.

Overall, Assistant is a powerful system that can help users plan a vacation and can help with a wide range of tasks and provide valuable insights and information on a wide range of topics.
"""

# COMMAND ----------

new_prompt = aibot.agent.create_prompt(
    system_message=sys_msg,
    tools=tools
)

aibot.agent.llm_chain.prompt = new_prompt

# COMMAND ----------

# MAGIC %md
# MAGIC By incorporating context from the Databricks Lakehouse including online tables and a feature serving endpoint, an AI chatbot created with context retrieval tools performs much better than a generic chatbot. 

# COMMAND ----------

aibot_output = aibot('Plan a 7-day vacation to the Amalfi Coast around September for user id "a-456".')

# COMMAND ----------

print(aibot_output['output'])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cleanup
# MAGIC Uncomment lines 2 - 5 in the following cell to clean up the endpoints created in this notebook.

# COMMAND ----------

# Delete endpoint
# status = fs.delete_feature_serving_endpoint(name=user_endpoint_name)
# print(status)
# status = fs.delete_feature_serving_endpoint(name=hotel_endpoint_name)
# print(status)


# Cleanup for online table from Unity Catalog