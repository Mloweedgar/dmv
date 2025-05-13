-- ============================================================================
-- process_wp_gps_data: Build water point report with GPS and location info
--
-- This procedure creates and populates:
--   * visualization.water_point_report_with_locations (created here)
-- by joining and enriching water point data with region, district, LGA, and village names and coordinates.
--
-- NOTE ON VISUALIZATION SCHEMA:
--   All tables in the visualization schema are derived and must be produced by a procedure.
--   For each visualization table dependency below, ensure the corresponding procedure has been run.
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * visualization.ruwasa_wp_report_vis (WARNING: No producing procedure found in db_functions. This table may have been created manually or outside the automated ETL process. This is a risk for automation and should be reviewed.)
--   * visualization.region_district_lga_names (produced by process_region_district_lga_names)
--   * foreign_schema_ruwasa_rsdms.ruwasa_villages (raw, external)
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure all source tables above are loaded and current (via ETL/import)
--   2. For each visualization.* dependency, run its producing procedure if the table is missing or stale:
--        - process_region_district_lga_names for visualization.region_district_lga_names
--        - (No producing procedure found for visualization.ruwasa_wp_report_vis; review and address as needed)
--   3. Run this procedure (process_wp_gps_data)
--
-- NOTE: If any dependency is missing or stale, output will be incomplete or incorrect.
--       This script is typically run after all upstream data processing is complete.
-- ============================================================================

CREATE OR REPLACE PROCEDURE public.process_wp_gps_data()
LANGUAGE plpgsql
AS $$
BEGIN
     --------------------------------------------------------------------------
    -- Step 1: Drop  water_point_report_with_locations Table if It Exists
    --------------------------------------------------------------------------
    DROP TABLE IF EXISTS visualization.water_point_report_with_locations;

    --------------------------------------------------------------------------
    -- Step 2: Create the water_point_report_with_locations table
    --------------------------------------------------------------------------
    CREATE TABLE visualization.water_point_report_with_locations AS
        SELECT DISTINCT ON (wp.dpid) 
            wp.dpid,
            (SELECT region_name FROM visualization.region_district_lga_names rdl 
            WHERE wp.region = rdl.region_code LIMIT 1) AS region_name,
            (SELECT district_name FROM visualization.region_district_lga_names rdl 
            WHERE wp.district_code = rdl.district_code LIMIT 1) AS district_name,
            (SELECT lga_name FROM visualization.region_district_lga_names rdl 
            WHERE wp.lga = rdl.lga_code LIMIT 1) AS lga_name,
            (SELECT villagename FROM foreign_schema_ruwasa_rsdms.ruwasa_villages v  
            WHERE wp.village = v.villagecode LIMIT 1) AS village_name,
            wp.ward AS ward_name,
            wp.functionalitystatus,
            wp.populationserved,
            wp.latitude,
            wp.longitude,
            wp.report_datetime,
            (SELECT geojson FROM visualization.region_district_lga_names rdl 
            WHERE wp.lga = rdl.lga_code LIMIT 1) AS geojson
        FROM visualization.ruwasa_wp_report_vis wp
        WHERE wp.latitude IS NOT NULL
        AND wp.longitude IS NOT NULL
        AND wp.functionalitystatus IS NOT NULL 
        AND wp.report_datetime >= date_trunc('month', current_date - interval '3 month')
        AND wp.report_datetime < date_trunc('month', current_date)
        AND wp.functionalitystatus IN('functional', 'not_functional', 'functional_need_repair') 
        ORDER BY wp.dpid, wp.report_datetime DESC;
        
END;
$$;

-- call process

CALL process_wp_gps_data();
