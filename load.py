import duckdb
import os
import logging
import time

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

def load_parquet_files():
    # create yellow/green/taxi list
    taxi_types = ['yellow', 'green']
    # create our months and years list
    months = [f"{i:02d}" for i in range(1,13)]
    years = [str(year) for year in range(2024, 2025)] # i had troubles loading 10 years of data, so if this would work it would stat from 2015 not 2024
    # base url
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet"
    
    con = None

    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        
        # Drop existing taxi_list tables
        for taxi_type in taxi_types:
            con.execute(f"DROP TABLE IF EXISTS {taxi_type}_trips")
        logger.info("Dropped taxi_trips ables if they existed")

                # Create tables for each taxi type
        for taxi_type in taxi_types:
            if taxi_type == 'yellow':
                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {taxi_type}_trips (
                        VendorID INTEGER,
                        tpep_pickup_datetime TIMESTAMP,
                        tpep_dropoff_datetime TIMESTAMP,
                        passenger_count INTEGER,
                        trip_distance DOUBLE
                    )
                """)
            else:  # green
                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {taxi_type}_trips (
                        VendorID INTEGER,
                        lpep_pickup_datetime TIMESTAMP,
                        lpep_dropoff_datetime TIMESTAMP,
                        passenger_count INTEGER,
                        trip_distance DOUBLE
                    )
                """)
            logger.info(f"Created table {taxi_type}_trips")        

        # Loop through each taxi type, year, and month to load data
        for taxi_type in taxi_types:
            for year in years:
                for month in months:
                    url = base_url.format(taxi_type=taxi_type, year=year, month=month)
                    try:
                        # rate limit to avoid overwhelming the server
                        time.sleep(1)  # Sleep for 1 second between requests
                        
                        if taxi_type == 'yellow':
                            con.execute(f"""
                                INSERT INTO {taxi_type}_trips 
                                SELECT VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance 
                                FROM read_parquet('{url}')
                            """)
                        else:  # green
                            con.execute(f"""
                                INSERT INTO {taxi_type}_trips 
                                SELECT VendorID, lpep_pickup_datetime, lpep_dropoff_datetime, passenger_count, trip_distance 
                                FROM read_parquet('{url}')
                            """)
                        
                        logger.info(f"Loaded data from {url} into {taxi_type}_trips table")
                    except Exception as e:
                        print(f"An error occurred: {e}")
                        logger.error(f"An error occurred: {e}")

        # Drop lookup table if it exists
        con.execute("DROP TABLE IF EXISTS taxi_lookup")
        logger.info("Dropped taxi_lookup table if it existed")
        # load in lookup table fom local csv data
        con.execute("""
            CREATE TABLE taxi_lookup AS 
            SELECT * FROM read_csv('data/vehicle_emissions.csv')
            """)
        logger.info("Loaded data into taxi_lookup table from local CSV")

        # print length of each table
        for table in ['yellow_trips', 'green_trips', 'taxi_lookup']:
            result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            print(f"Raw {table} has {result[0]} records")
            logger.info(f"Raw {table} has {result[0]} records")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
            # Always close the connection
            if con:
                con.close()
                logger.info("Database connection closed")

if __name__ == "__main__":
    load_parquet_files()  
