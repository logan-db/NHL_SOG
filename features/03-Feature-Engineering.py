# Databricks notebook source
from databricks.feature_store import feature_table
import pyspark.pandas as ps
 
def compute_churn_features(data):
  
  # Convert to a dataframe compatible with the pandas API
  data = data.pandas_api()
  
  # OHE
  data = ps.get_dummies(data, 
                        columns=['gender', 'partner', 'dependents',
                                 'phone_service', 'multiple_lines', 'internet_service',
                                 'online_security', 'online_backup', 'device_protection',
                                 'tech_support', 'streaming_tv', 'streaming_movies',
                                 'contract', 'paperless_billing', 'payment_method'], dtype = 'int64')
  
  # Convert label to int and rename column
  data['churn'] = data['churn'].map({'Yes': 1, 'No': 0})
  data = data.astype({'churn': 'int32'})
  
  # Clean up column names
  data.columns = [re.sub(r'[\(\)]', ' ', name).lower() for name in data.columns]
  data.columns = [re.sub(r'[ -]', '_', name).lower() for name in data.columns]
 
  
  # Drop missing values
  data = data.dropna()
  
  return data

# COMMAND ----------

from databricks.feature_store import FeatureStoreClient
 
fs = FeatureStoreClient()
 
churn_features_df = compute_churn_features(telcoDF)
 
try:
  #drop table if exists
  fs.drop_table(f'{dbName}.dbdemos_mlops_churn_features')
except:
  pass
#Note: You might need to delete the FS table using the UI
churn_feature_table = fs.create_table(
  name=f'{dbName}.dbdemos_mlops_churn_features',
  primary_keys='customer_id',
  schema=churn_features_df.spark.schema(),
  description='These features are derived from the churn_bronze_customers table in the lakehouse.  We created dummy variables for the categorical columns, cleaned up their names, and added a boolean flag for whether the customer churned or not.  No aggregations were performed.'
)
 
fs.write_table(df=churn_features_df.to_spark(), name=f'{dbName}.dbdemos_mlops_churn_features', mode='overwrite')

