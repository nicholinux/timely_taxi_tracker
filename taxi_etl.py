import pandas as pd
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta
import duckdb

# Datasets are always published with the following link format:
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet

def get_old_data():
    """ Getter for existing NYC taxi parquet files (2009 - 2025) """
    urls = []
    for year in range(2009, 2025):
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

def get_daily_revenue(con, start_date, end_date):
    """ Getter for daily revenue """
    df = con.execute(
        """
        SELECT
            DATE(tpep_pickup_datetime) AS trip_date,
            SUM(total_amount) AS total_revenue
        FROM trips
        WHERE tpep_pickup_datetime >= ?
         AND tpep_pickup_datetime <  ?
        GROUP BY trip_date
        ORDER BY trip_date ASC;
        """, 
        [start_date, end_date]
    ).df()

    return df

def get_trips_per_day(con, start_date=None, end_date=None):
    """ Getter for trips per day """
    df = con.execute(
        """
        SELECT
            DATE(tpep_pickup_datetime) AS trip_date,
            COUNT(*) AS trip_count,
        FROM trips
        WHERE tpep_pickup_datetime >= ?
         AND tpep_pickup_datetime < ?
        GROUP BY trip_date
        ORDER BY trip_count ASC;
        """, 
        [start_date, end_date]
    ).df()

    return df

def get_trips_per_hour(con, start_date, end_date):
    """ Getter for number of trips per hour over specified period of time """
    df = con.execute(
        """
        SELECT
            EXTRACT(HOUR FROM tpep_pickup_datetime) AS pickup_hour,
            AVG(
                EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60
            ) AS avg_trip_minutes
        FROM trips
        WHERE tpep_pickup_datetime >= ?
         AND tpep_dropoff_datetime < ?
        GROUP BY pickup_hour
        ORDER BY pickup_hour;    
        """,
        [start_date, end_date]
    ).df()

    return df

def get_avg_fare(con, start_date=None, end_date=None):
    """ Getter for avg fare over specified period of time """
    df = con.execute(
        """
        SELECT
            DATE(tpep_pickup_datetime) AS trip_date,
            AVG(total_amount) as avg_fare
        FROM trips
        WHERE tpep_pickup_datetime >= ?
         AND tpep_pickup_datetime < ?
        GROUP BY trip_date
        ORDER BY trip_date ASC;
        """,
        [start_date, end_date]
    ).df()

    return df

def get_avg_trip_duration(con, start_date, end_date):
    """ Getter for avg trip duration over specified period of time """
    df = con.execute(
        """
       SELECT
            AVG(
                EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60
            ) AS avg_trip_minutes
        FROM trips
        WHERE tpep_pickup_datetime >= ?
        AND tpep_pickup_datetime < ?;
        """,
        [start_date, end_date]
    ).df()

    return df



    

    




