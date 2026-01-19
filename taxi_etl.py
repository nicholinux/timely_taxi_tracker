import pandas as pd
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta

# Datasets are always published with the following link format:
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet



def get_old_data():
    """ Getter for existing NYC taxi parquet files (2009 - 2025) """
    urls = []
    for year in range(2023, 2025):
        for month in range(1, 13):
            formatted_month = f"{month:02}"
            url = (f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{formatted_month}.parquet")
            urls.append(url)

def clean_parquet_data(monthly_parquet_url):
    """ Drops unneeded columns and enforces date/time consistency """
    year = monthly_parquet_url.split('_')[-1].split('-')[0]
    month = monthly_parquet_url.split('_')[-1].split('-')[1].split('.')[0]

    print(f"Cleaning data for {year}-{month}")

    df = pd.read_parquet(monthly_parquet_url)
    df = df.drop(columns=['extra', 'store_and_fwd_flag', 'RatecodeID', 'PULocationID'])

    df = df[df['trip_distance'] > 0]
    df = df[df['total_amount'] > 0]
    df = df[df['congestion_surcharge'] >= 0]

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.to_parquet('C:/Users/nickh/timely_taxi_tracker/data/taxi_cleaned_data/taxi_cleaned_data.parquet')

    print(f"Finished cleaning data for {year}-{month}")
    

    




