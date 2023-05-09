import pandas as pd
import numpy as np
import requests
import time
import os
from datetime import date, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'groovy-smithy-386015-6898ff63f9b9.json'


def get_data_from_BigQuery(table):


 #   client = bigquery.Client()


    sql_query= """
    -- This query shows a list of the daily top Google Search terms.

    SELECT  *
    FROM `{}.{}.{}`
    ORDER BY period, respondent, respondent_name

    """.format(table.project,table.dataset_id, table.table_id)

    query_job = client.query(sql_query)

    df_bigquery = query_job.to_dataframe()


    return df_bigquery


def aggregate_data_fetched_from_bigquery(dataset_id):
    tables = client.list_tables(dataset_id)  # Make an API request.
    df = []
    for table in tables:
        print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        df.append(get_data_from_BigQuery(table))

    df = pd.concat(df)
    return df


def merge_data_and_preprocess(df_1, df_2):
    df_combined = pd.merge(df_1, df_2, how='inner', on = ['period' ,	'respondent'])
    columns_of_interest = ['period', 'respondent', 'respondent_name_x',
                       'Day_ahead_demand_forecast', 'Demand', 'Net_generation', 'Total_interchange',
                       'Natural_gas', 'Coal', 'Wind', 'Hydro', 'Solar', 'Petroleum', 'Nuclear', 'Other']
    
    df_combined = df_combined[columns_of_interest]
    df_combined = df_combined.reset_index(drop=True)

    return df_combined


    



if __name__ == '__main__':
    client = bigquery.Client()

    df_demand = aggregate_data_fetched_from_bigquery(dataset_id='groovy-smithy-386015.electricity')
    df_source = aggregate_data_fetched_from_bigquery(dataset_id='groovy-smithy-386015.source')    

    df_final = merge_data_and_preprocess(df_demand,df_source)

    df_final.to_parquet('data/data.parquet')


