# Timely Taxi Tracker

Ingests live data from the NYC Yellow Taxi dataset that is updated every month.

An ETL pipeline cleans and aggregates the parquet data into one file. DuckDB is then used to answer essential analytics questions, such as total revenue by day/month/hour.

These queries will then be mapped onto a live dashboard powered by streamlit. 

# To-do List:

- ~~Extract URLs for parquet files corresponding to years 2009 to 2025~~
- Write function to live probe for URLs to ingest newly added data from 2026-01 and onward
- ~~Write function to clean these files on a month-by-month basis~~
- Create aggregated parquet file
- Write DuckDB queries to answer analytics questions
- Create streamlit dashboard to display findings
