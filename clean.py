import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean.log'
)
logger = logging.getLogger(__name__)

con = None

def clean():
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")
        
        # use loop to perform same cleaning operations on both taxi tables
        for taxi_type in ['yellow', 'green']:
            # deduplicate yellow and green taxi tables
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {taxi_type}_trips AS
                SELECT DISTINCT * FROM {taxi_type}_trips
            """)
            logger.info(f"Created deduplicated table {taxi_type}_trips")

            # remove trips with 0 passengers
            con.execute(f"""
                DELETE FROM {taxi_type}_trips
                WHERE passenger_count = 0
            """)
            logger.info(f"Removed trips with 0 passengers from {taxi_type}_trips")

            # remove trips 0 miles in length
            con.execute(f"""
                DELETE FROM {taxi_type}_trips
                WHERE trip_distance = 0
            """)
            logger.info(f"Removed trips with 0 miles from {taxi_type}_trips")

            # remove trips greater than 100 miles in length
            con.execute(f"""
                DELETE FROM {taxi_type}_trips
                WHERE trip_distance > 100
            """)
            logger.info(f"Removed trips greater than 100 miles from {taxi_type}_trips")

            # remove trips longer than 24 hours (use if condition because of different datetime column names)
            if taxi_type == 'yellow':
                con.execute(f"""
                    DELETE FROM {taxi_type}_trips
                    WHERE EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600 > 24
                """)
            else:  # green
                con.execute(f"""
                    DELETE FROM {taxi_type}_trips
                    WHERE EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600 > 24
                """)
            logger.info(f"Removed trips longer than 24 hours from {taxi_type}_trips")
            
            # tests to verify cleaning steps
            # 0 passenger count check
            zero_passenger_count = con.execute(f"""
                SELECT COUNT(*) FROM {taxi_type}_trips
                WHERE passenger_count = 0
            """).fetchone()[0]
            print(f"{taxi_type} trips with 0 passengers: {zero_passenger_count}")

            # 0 mile trip check
            logger.info(f"{taxi_type} trips with 0 passengers: {zero_passenger_count}")
            zero_mile_trips = con.execute(f"""
                SELECT COUNT(*) FROM {taxi_type}_trips
                WHERE trip_distance = 0
            """).fetchone()[0]
            print(f"{taxi_type} trips with 0 miles: {zero_mile_trips}")
            logger.info(f"{taxi_type} trips with 0 miles: {zero_mile_trips}")

            # over 100 mile trip check
            over_100_mile_trips = con.execute(f"""
                SELECT COUNT(*) FROM {taxi_type}_trips
                WHERE trip_distance > 100
            """).fetchone()[0]
            print(f"{taxi_type} trips over 100 miles: {over_100_mile_trips}")
            logger.info(f"{taxi_type} trips over 100 miles: {over_100_mile_trips}")
            
            # over 24 hour trip check (using EXTRACT EPOCH instead of julianday)
            if taxi_type == 'yellow':
                over_24_hour_trips = con.execute(f"""
                    SELECT COUNT(*) FROM {taxi_type}_trips
                    WHERE EXTRACT(EPOCH FROM (tpep_dropoff_datetime - tpep_pickup_datetime)) / 3600 > 24
                """).fetchone()[0]
            else:  # green
                over_24_hour_trips = con.execute(f"""
                    SELECT COUNT(*) FROM {taxi_type}_trips
                    WHERE EXTRACT(EPOCH FROM (lpep_dropoff_datetime - lpep_pickup_datetime)) / 3600 > 24
                """).fetchone()[0]
            print(f"{taxi_type} trips over 24 hours: {over_24_hour_trips}")
            logger.info(f"{taxi_type} trips over 24 hours: {over_24_hour_trips}")

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")
    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    clean()