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

