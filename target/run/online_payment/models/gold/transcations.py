
  
    
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('smallTest').getOrCreate()

spark.conf.set("viewsEnabled","true")
spark.conf.set("temporaryGcsBucket","df11_online_payment")

import pandas as pd
from google.cloud import bigquery

def model(dbt, session):

    dbt.config(
        submission_method="cluster",
        dataproc_cluster_name="dbt-python"
    )

    transactions_df = dbt.ref("brz_online_payment_add_id")

    transactions = transactions_df.to_pandas()

    # apply our function
    # (columns need to be in uppercase on Snowpark)
    transactions = transactions_df['payment_id', 'payment_datetime', 'amount', 'type', 'isFraud']

    # return final dataset (Pandas DataFrame)
    return transactions


# # Initialize a BigQuery client
# client = bigquery.Client()

# # Construct a reference to the dataset and table
# table_ref = client.dataset('online_payment_raw').table('online_payment_add_id')

# # Selecting required columns
# transaction = table_ref[['payment_id', 'payment_datetime', 'amount', 'type', 'isFraud']]

# # Displaying the resulting DataFrame
# print(transaction)


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {"brz_online_payment_add_id": "final-project-404104.online_payment_raw.brz_online_payment_add_id"}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "final-project-404104"
    schema = "online_payment_marts"
    identifier = "transcations"
    
    def __repr__(self):
        return 'final-project-404104.online_payment_marts.transcations'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------



dbt = dbtObj(spark.read.format("bigquery").load)
df = model(dbt, spark)

# COMMAND ----------
# this is materialization code dbt generated, please do not modify

import pyspark
# make sure pandas exists before using it
try:
  import pandas
  pandas_available = True
except ImportError:
  pandas_available = False

# make sure pyspark.pandas exists before using it
try:
  import pyspark.pandas
  pyspark_pandas_api_available = True
except ImportError:
  pyspark_pandas_api_available = False

# make sure databricks.koalas exists before using it
try:
  import databricks.koalas
  koalas_available = True
except ImportError:
  koalas_available = False

# preferentially convert pandas DataFrames to pandas-on-Spark or Koalas DataFrames first
# since they know how to convert pandas DataFrames better than `spark.createDataFrame(df)`
# and converting from pandas-on-Spark to Spark DataFrame has no overhead
if pyspark_pandas_api_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = pyspark.pandas.frame.DataFrame(df)
elif koalas_available and pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = databricks.koalas.frame.DataFrame(df)

# convert to pyspark.sql.dataframe.DataFrame
if isinstance(df, pyspark.sql.dataframe.DataFrame):
  pass  # since it is already a Spark DataFrame
elif pyspark_pandas_api_available and isinstance(df, pyspark.pandas.frame.DataFrame):
  df = df.to_spark()
elif koalas_available and isinstance(df, databricks.koalas.frame.DataFrame):
  df = df.to_spark()
elif pandas_available and isinstance(df, pandas.core.frame.DataFrame):
  df = spark.createDataFrame(df)
else:
  msg = f"{type(df)} is not a supported type for dbt Python materialization"
  raise Exception(msg)

df.write \
  .mode("overwrite") \
  .format("bigquery") \
  .option("writeMethod", "direct").option("writeDisposition", 'WRITE_TRUNCATE') \
  .save("final-project-404104.online_payment_marts.transcations")

  