import duckdb
from pathlib import Path

DB_PATH = Path("heatgrid.duckdb")

def build_noaa_daily():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    #daily max and mean temp per station
    con.execute(f"""DROP TABLE IF EXISTS noaa_daily_temp;
                """)
    con.execute(f"""CREATE TABLE noaa_daily_temp AS
                SELECT 
                    station,
                    DATE_TRUNC('day', hour_utc) AS day_utc,
                    MAX(temp_C) AS daily_max_temp_C,
                    AVG(temp_C) AS avg_temp_C
                FROM noaa_hourly_avg
                GROUP BY station, day_utc
                ORDER BY station, day_utc;""")
    print("noaa_daily_temp rows: ", con.execute("SELECT COUNT(*) FROM noaa_daily_temp;").fetchone()[0])

    con.close()

#we now have one row per day perstation
#we'll compute the 90th percentile thresholds per station and day of the year
#"for each station, what is the 90th percentile of daily max temps for that day of year?"
def build_noaa_thresholds():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    con.execute(f"""DROP TABLE IF EXISTS noaa_temp_thresholds;
                """)
    con.execute(f"""CREATE TABLE noaa_temp_thresholds AS
                SELECT 
                    station,
                    strftime(day_utc, '%m-%d') AS mmdd,
                    quantile_cont(daily_max_temp_C, 0.90) AS t90_max
                FROM noaa_daily_temp
                GROUP BY station, mmdd;
            """)
    print("noaa_temp_thresholds rows: ", con.execute("SELECT COUNT(*) FROM noaa_temp_thresholds;").fetchone()[0])

    con.close()

#for each day, check if daily max temp >= 90th percentile threshold for that station and day of year
def build_noaa_heatwave_flags():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    con.execute("""
        DROP TABLE IF EXISTS noaa_heatwave_flags;
    """)

    con.execute("""
        CREATE TABLE noaa_heatwave_flags AS
        WITH daily_with_mmdd AS (
            SELECT
                station,
                day_utc,
                daily_max_temp_C,
                avg_temp_C,
                strftime(day_utc, '%m-%d') AS mmdd
            FROM noaa_daily_temp
        )
        SELECT
            d.station,
            d.day_utc,
            d.daily_max_temp_C,
            d.avg_temp_C,
            t.t90_max,
            CASE 
                WHEN d.daily_max_temp_C >= t.t90_max THEN 1
                ELSE 0
            END AS is_hot_day
        FROM daily_with_mmdd d
        JOIN noaa_temp_thresholds t
          ON d.station = t.station
         AND d.mmdd = t.mmdd
        ORDER BY d.station, d.day_utc;
    """)

    print("noaa_heatwave_flags rows:",
          con.execute("SELECT COUNT(*) FROM noaa_heatwave_flags;").fetchone()[0])

    #how many hot vs non-hot days?
    print(con.execute("""
        SELECT station, is_hot_day, COUNT(*) 
        FROM noaa_heatwave_flags
        GROUP BY station, is_hot_day
        ORDER BY station, is_hot_day;
    """).fetchall())

    con.close()

#finding the total energy consumption per day and the peak hour of day
def build_eia_daily_load():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    con.execute("DROP TABLE IF EXISTS eia_daily_load;")

    # FIX: remove any duplicate region/hour rows before aggregating to days
    con.execute("""
        CREATE TABLE eia_daily_load AS
        WITH hourly_unique AS (
            SELECT DISTINCT
                region,
                hour_utc,
                load_mwh
            FROM eia_load_hourly
        )
        SELECT
            region,
            date_trunc('day', hour_utc) AS day_utc,
            SUM(load_mwh) AS daily_total_mwh,   -- total energy used that day
            MAX(load_mwh) AS daily_peak_mwh     -- highest hourly demand that day
        FROM hourly_unique
        GROUP BY region, day_utc
        ORDER BY region, day_utc;
    """)

    print(
        "eia_daily_load rows:",
        con.execute("SELECT COUNT(*) FROM eia_daily_load;").fetchone()[0]
    )
    con.close()


def heat_load_daily():
    con = duckdb.connect(str(DB_PATH), read_only=False)

    con.execute("DROP TABLE IF EXISTS heat_load_daily;")
    con.execute("""
        CREATE TABLE heat_load_daily AS
            SELECT
                n.station,
                CASE WHEN n.station = 'IAD' THEN 'PJM'
                    WHEN n.station = 'BOS' THEN 'ISNE'
                END AS region,
                n.day_utc,
                n.daily_max_temp_C,
                n.avg_temp_C,
                t.t90_max,
                (n.daily_max_temp_C >= t.t90_max) AS is_hot_day,
                e.daily_total_mwh,
                e.daily_peak_mwh
            FROM noaa_daily_temp n
            LEFT JOIN noaa_temp_thresholds t
                ON n.station = t.station
            AND strftime(n.day_utc, '%m-%d') = t.mmdd
            LEFT JOIN eia_daily_load e
                ON e.region = CASE WHEN n.station = 'IAD' THEN 'PJM' ELSE 'ISNE' END
            AND e.day_utc = n.day_utc;
        """)

    print(
        "heat_load_daily rows:",
        con.execute("SELECT COUNT(*) FROM heat_load_daily;").fetchone()[0]
    )
    con.close()

    
if __name__ == "__main__":
    build_noaa_daily()
    build_noaa_thresholds()
    build_noaa_heatwave_flags()
    build_eia_daily_load()
    heat_load_daily()
