import duckdb
import logging


# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/clean_va.log",
)
logger = logging.getLogger(__name__)
logger.info("--------- New Cleaning run ---------")

# establish connection
con = duckdb.connect('heatgrid.duckdb')
logger.info("Connected to DuckDB instance")


# Drop table if exists
con.execute("""
            DROP TABLE IF EXISTS cleaned_va_datacenters;
 """)

# Create cleaned table with appropriate data types and transformations
con.execute("""
            CREATE TABLE cleaned_va_datacenters AS
            SELECT
                -- Identity
                company,
                brand,
                city,
                county,
                address,
                zip,
                geometry,
            
            -- Power capacity and estimated loads (convert kW to MW)
            TRY_CAST(REPLACE("total generator rate capacity kw", ',', '') AS DOUBLE) / 1000.0 AS capacity_mw,
            TRY_CAST(REPLACE("estimate power consumption in kw/hr (calculated 30%)", ',', '') AS DOUBLE) / 1000.0 AS est_load_30_mw,
            TRY_CAST(REPLACE("estimate power consumption in kw/hr (calculated 50%)", ',', '') AS DOUBLE) / 1000.0 AS est_load_50_mw,
            TRY_CAST(REPLACE("estimate power consumption in kw/hr (calculated 60%)", ',', '') AS DOUBLE) / 1000.0 AS est_load_60_mw,

            -- Annual consumption at 50% (the main one you'll use)
            TRY_CAST(REPLACE("estimated data center electricity use in terrawatt-hours a year at 50% capacity", ',', '') AS DOUBLE) AS annual_load_50_twh,
    
            
            -- Community exposure
            TRY_CAST(REPLACE("total population within 1 mile of site", ',', '') AS INTEGER) AS pop_within_mile,
            
            -- Environmental impact
            TRY_CAST(REPLACE("co tpy", ',', '') AS DOUBLE) AS co_tons_per_year,
            "water stress" AS water_stress_index,
            
            -- Temporal
            TRY_CAST("data center construct year" AS INTEGER) AS construct_year,
            
            -- Environmental Justice Indices (Air Quality)
            TRY_CAST("us percentile for ej index for particulate matter" AS DOUBLE) AS ej_particulate_matter,
            TRY_CAST("us percentile for ej index for ozone" AS DOUBLE) AS ej_ozone,
            TRY_CAST("us percentile for ej index for diesel particulate matter" AS DOUBLE) AS ej_diesel_pm,
            TRY_CAST("us percentile for ej index for nitrogen dioxide (no2)" AS DOUBLE) AS ej_nitrogen_dioxide,
            
            -- Environmental Justice Indices (Proximity Hazards)
            TRY_CAST("us percentile for ej index for traffic proximity and volume" AS DOUBLE) AS ej_traffic,
            TRY_CAST("us percentile for ej index for hazardous waste proximity" AS DOUBLE) AS ej_hazardous_waste,
            TRY_CAST("us percentile for ej index for superfund proximity" AS DOUBLE) AS ej_superfund,
            TRY_CAST("us percentile for ej index for rmp proximity" AS DOUBLE) AS ej_rmp,
            TRY_CAST("us percentile for ej index for underground storage tanks (ust) indicator" AS DOUBLE) AS ej_ust,
            
            -- Environmental Justice Indices (Contaminants)
            TRY_CAST("us percentile for ej index for toxic releases to air" AS DOUBLE) AS ej_toxic_air,
            TRY_CAST("us percentile for ej index for wastewater discharge indicator" AS DOUBLE) AS ej_wastewater,
            TRY_CAST("us percentile for ej index for lead paint indicator" AS DOUBLE) AS ej_lead_paint,
            TRY_CAST("us percentile for ej index for drinking water non-compliance" AS DOUBLE) AS ej_drinking_water
            
            FROM data_centers_va
            WHERE county IS NOT NULL
                AND county IN ('Loudoun', 'Fairfax', 'Prince William', 'Culpeper', 'Fauquier');
                
 """)

# Add lat/lon columns
con.execute("""
            INSTALL spatial;
            LOAD spatial;

            ALTER TABLE cleaned_va_datacenters ADD COLUMN lat DOUBLE;
            ALTER TABLE cleaned_va_datacenters ADD COLUMN lon DOUBLE;

            UPDATE cleaned_va_datacenters
            SET 
                lat = ST_Y(ST_GeomFromWKB(geometry)),
                lon = ST_X(ST_GeomFromWKB(geometry))
            WHERE geometry IS NOT NULL;
""")