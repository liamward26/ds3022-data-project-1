import duckdb
import logging

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log'
)
logger = logging.getLogger(__name__)

con = None

def analysis():
    try:
        # Connect to local DuckDB instance
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")

        # loop for each trip table
        for taxi_type in ['yellow', 'green']:
            # largest carbon producing trip
            result = con.execute(f"""
                SELECT trip_co2_kgs
                FROM {taxi_type}_trips
                ORDER BY trip_co2_kgs DESC
                LIMIT 1
            """).fetchone()
            
            if result:
                logger.info(f"Largest carbon producing trip for {taxi_type} taxi: {result[0]}")
                print(f"Largest carbon producing trip for {taxi_type} taxi: {result[0]}")
            else:
                logger.warning(f"No data found for {taxi_type} taxi trips.")
                print(f"No data found for {taxi_type} taxi trips.")

        # most carbon heavy and carbon light hours of the day for YELLOW and for GREEN trips (i'm going to keep using similar for loops)
        for taxi_type in ['yellow', 'green']:
            # Get most carbon heavy hour (group by hour of day order by trip_co2_kgs)
            heavy_hour = con.execute(f"""
                SELECT hour_of_day
                FROM {taxi_type}_trips
                GROUP BY hour_of_day
                ORDER BY SUM(trip_co2_kgs) DESC
                LIMIT 1
            """).fetchone()
            
            # Get most carbon light hour (same as above but ascending order)
            light_hour = con.execute(f"""
                SELECT hour_of_day
                FROM {taxi_type}_trips
                GROUP BY hour_of_day
                ORDER BY SUM(trip_co2_kgs) ASC
                LIMIT 1
            """).fetchone()

            # again the same but for day of week (add a little logic to convert number to day name)
            heavy_day = con.execute(f"""
                SELECT CASE day_of_week
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday' 
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                    WHEN 0 THEN 'Sunday'
                END as day_name
                FROM {taxi_type}_trips
                GROUP BY day_of_week
                ORDER BY SUM(trip_co2_kgs) DESC
                LIMIT 1
            """).fetchone()
            
            # Get most carbon light day (same as above)
            light_day = con.execute(f"""
                SELECT CASE day_of_week
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday' 
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                    WHEN 0 THEN 'Sunday'
                END as day_name
                FROM {taxi_type}_trips
                GROUP BY day_of_week
                ORDER BY SUM(trip_co2_kgs) ASC
                LIMIT 1
            """).fetchone()

            # carbon heavy week (again very similar to above but group by week number)
            heavy_week = con.execute(f"""
                SELECT week_of_year
                FROM {taxi_type}_trips
                GROUP BY week_of_year
                ORDER BY SUM(trip_co2_kgs) DESC
                LIMIT 1
            """).fetchone()

            # carbon light week (same as above)
            light_week = con.execute(f"""
                SELECT week_of_year
                FROM {taxi_type}_trips
                GROUP BY week_of_year
                ORDER BY SUM(trip_co2_kgs) ASC
                LIMIT 1
            """).fetchone()

            # carbon heavy month (again very similar to above but group by month number (we also need to convert month number to month name))
            heavy_month = con.execute(f"""
                SELECT CASE month_of_year
                    WHEN 1 THEN 'January'
                    WHEN 2 THEN 'February' 
                    WHEN 3 THEN 'March'
                    WHEN 4 THEN 'April'
                    WHEN 5 THEN 'May'
                    WHEN 6 THEN 'June'
                    WHEN 7 THEN 'July'
                    WHEN 8 THEN 'August'
                    WHEN 9 THEN 'September'
                    WHEN 10 THEN 'October'
                    WHEN 11 THEN 'November'
                    WHEN 12 THEN 'December'
                END as month_name
                FROM {taxi_type}_trips
                GROUP BY month_of_year
                ORDER BY SUM(trip_co2_kgs) DESC
                LIMIT 1
            """).fetchone()

            # carbon light month (same as above)
            light_month = con.execute(f"""
                SELECT CASE month_of_year
                    WHEN 1 THEN 'January'
                    WHEN 2 THEN 'February' 
                    WHEN 3 THEN 'March'
                    WHEN 4 THEN 'April'
                    WHEN 5 THEN 'May'
                    WHEN 6 THEN 'June'
                    WHEN 7 THEN 'July'
                    WHEN 8 THEN 'August'
                    WHEN 9 THEN 'September'
                    WHEN 10 THEN 'October'
                    WHEN 11 THEN 'November'
                    WHEN 12 THEN 'December'
                END as month_name
                FROM {taxi_type}_trips
                GROUP BY month_of_year
                ORDER BY SUM(trip_co2_kgs) ASC
                LIMIT 1
            """).fetchone()

            # print and log results
            if heavy_hour and light_hour and heavy_day and light_day:
                logger.info(f"Most carbon heavy hour for {taxi_type} taxi: {heavy_hour[0]}")
                logger.info(f"Most carbon light hour for {taxi_type} taxi: {light_hour[0]}")
                logger.info(f"Most carbon heavy day for {taxi_type} taxi: {heavy_day[0]}")
                logger.info(f"Most carbon light day for {taxi_type} taxi: {light_day[0]}")
                logger.info(f"Most carbon heavy week for {taxi_type} taxi: {heavy_week[0]}")
                logger.info(f"Most carbon light week for {taxi_type} taxi: {light_week[0]}")
                logger.info(f"Most carbon heavy month for {taxi_type} taxi: {heavy_month[0]}")
                logger.info(f"Most carbon light month for {taxi_type} taxi: {light_month[0]}")
                print(f"Most carbon heavy hour for {taxi_type} taxi: {heavy_hour[0]}")
                print(f"Most carbon light hour for {taxi_type} taxi: {light_hour[0]}")
                print(f"Most carbon heavy day for {taxi_type} taxi: {heavy_day[0]}")
                print(f"Most carbon light day for {taxi_type} taxi: {light_day[0]}")
                print(f"Most carbon heavy week for {taxi_type} taxi: {heavy_week[0]}")
                print(f"Most carbon light week for {taxi_type} taxi: {light_week[0]}")
                print(f"Most carbon heavy month for {taxi_type} taxi: {heavy_month[0]}")
                print(f"Most carbon light month for {taxi_type} taxi: {light_month[0]}")

            else:
                logger.info(f"No data found for {taxi_type} taxi trips.")
                print(f"No data found for {taxi_type} taxi trips.")

            #Use a plotting library of your choice (matplotlib, seaborn, etc.) to generate a time-series plot or histogram with MONTH along the X-axis and CO2 totals along the Y-axis. Render two lines/bars/plots of data, one each for YELLOW and GREEN taxi trip CO2 totals.
        try:
            import matplotlib.pyplot as plt

            # Query to get monthly CO2 totals for both taxi types
            yellow_data = con.execute("""
                SELECT month_of_year, SUM(trip_co2_kgs) AS trip_co2_kgs
                FROM yellow_trips
                GROUP BY month_of_year # again if we were able to load the 10 years of data this would be year instead of month
                ORDER BY month_of_year
            """).fetchall()

            green_data = con.execute("""
                SELECT month_of_year, SUM(trip_co2_kgs) AS trip_co2_kgs
                FROM green_trips
                GROUP BY month_of_year
                ORDER BY month_of_year
            """).fetchall()

            # Prepare data for plotting
            yellow_months = [row[0] for row in yellow_data]
            yellow_totals = [row[1] for row in yellow_data]
            green_months = [row[0] for row in green_data]
            green_totals = [row[1] for row in green_data]

            # Create separate plots
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

            # Yellow taxi plot
            ax1.plot(yellow_months, yellow_totals, marker='o', label='Yellow Taxi', color='orange', linewidth=2)
            ax1.set_title('Monthly CO2 Totals - Yellow Taxi')
            ax1.set_xlabel('Month of Year')
            ax1.set_ylabel('CO2 Total')
            ax1.set_xticks(yellow_months)
            ax1.grid(True, alpha=0.3)

            # Green taxi plot
            ax2.plot(green_months, green_totals, marker='o', label='Green Taxi', color='green', linewidth=2)
            ax2.set_title('Monthly CO2 Totals - Green Taxi')
            ax2.set_xlabel('Month of Year')
            ax2.set_ylabel('CO2 Total')
            ax2.set_xticks(green_months)
            ax2.grid(True, alpha=0.3)

            plt.tight_layout()
            plt.savefig('monthly_co2_totals_separate.png', dpi=300, bbox_inches='tight')
            plt.show()
            logger.info("Generated and saved separate monthly CO2 totals plots")
    
        except Exception as e:
            logger.error(f"Error creating plots: {e}")
            print(f"Error creating plots: {e}")

    except Exception as e:
        logger.error(f"Error during analysis: {e}")
    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    analysis()