import duckdb

DB_PATH = "analytics.duckdb"
PARQUET_PATH = "data/taxi_cleaned_data/tax_cleaned_data.parquet"

def get_connection():
    con = duckdb.connect(DB_PATH)
    con.execute(f"""
        CREATE OR REPLACE VIEW trips AS
        SELECT *
        FROM read_parquet('{PARQUET_PATH}')
    """)
    return con
