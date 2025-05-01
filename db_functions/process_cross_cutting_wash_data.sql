-- ============================================================================
-- process_cross_cutting_wash_data: Build cross-sector WASH visualization tables
--
-- This procedure creates and populates:
--   * visualization.cross_cutting_wash_data_vis (main output, created here)
--   * visualization.ruwasa_service_level_lga (intermediate, created here)
--   * visualization.nsmis_household_sanitation_reports_lga (summary, created here)
-- by aggregating and joining data from NSMIS, RUWASA, and spatial reference tables.
--
-- NOTE ON VISUALIZATION SCHEMA:
--   All tables in the visualization schema are derived and must be produced by a procedure.
--   For each visualization table dependency below, ensure the corresponding procedure has been run.
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * foreign_schema_ruwasa_rsdms.ruwasa_villages (raw, external)
--   * visualization.ruwasa_lgas_with_geojson (produced by process_ruwasa_lgas_with_geojson)
--   * visualization.nsmis_household_sanitation_reports_vis (produced by process_nsmis_data)
--   * visualization.ruwasa_wps_district (produced by process_ruwasa_wp_report)
--   * public.ruwasa_districts (raw, external)
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure all source tables above are loaded and current (via ETL/import)
--   2. For each visualization.* dependency, run its producing procedure if the table is missing or stale:
--        - process_ruwasa_lgas_with_geojson for visualization.ruwasa_lgas_with_geojson
--        - process_nsmis_data for visualization.nsmis_household_sanitation_reports_vis
--        - process_ruwasa_wp_report for visualization.ruwasa_wps_district
--        - (and so on for other visualization.* dependencies)
--   3. Run this procedure (process_cross_cutting_wash_data)
--
-- NOTE: If any dependency is missing or stale, output will be incomplete or incorrect.
--       This script is typically run after all upstream data processing is complete (e.g., after NSMIS, BEMIS, and RSDMS data are processed).
--       Recommended schedule: monthly.
-- ============================================================================


CREATE OR REPLACE PROCEDURE process_cross_cutting_wash_data()
LANGUAGE plpgsql
AS $$
BEGIN

    --------------------------------------------------------------------------
    -- Step 1: Collapse village level data to LGA level before joins
    --------------------------------------------------------------------------

    DROP TABLE IF EXISTS visualization.ruwasa_service_level_lga; 
    CREATE TABLE visualization.ruwasa_service_level_lga AS
    SELECT lgacode, AVG(infracoverage) AS lga_water_access_level_perc
    FROM foreign_schema_ruwasa_rsdms.ruwasa_villages
    GROUP BY lgacode;

    --------------------------------------------------------------------------
    -- Step 2: Drop Existing Visualization Table if It Exists
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.cross_cutting_wash_data_vis';
    
    --------------------------------------------------------------------------
    -- Step 3: Create the Visualization Table with Joined NSMIS and RUWASA LGA with names
    --------------------------------------------------------------------------
    DROP TABLE IF EXISTS visualization.cross_cutting_wash_data_vis;
    
    CREATE TABLE visualization.cross_cutting_wash_data_vis AS  
    SELECT 
        l.lgacode,  
        l.nsmislgacode,
        l.bemislgacode,
        l.lganame,
        l.districtcode,
        l.geojson,
        (SELECT rd.districtname FROM public.ruwasa_districts rd where rd.districtcode = l.districtcode) district_name,
        ROUND(AVG(n.improved_perc_hhs::NUMERIC)*100, 1) AS lga_improved_san_perc_hhs, 
        ROUND(AVG(n.handwashstation_perc_hhs::NUMERIC)*100, 1) AS lga_handwashstation_perc_hhs,
        ROUND(AVG(n.handwashsoap_perc_hhs::NUMERIC)*100,1) AS lga_handwashsoap_perc_hhs,
        FIRST_VALUE(n.regioncode) OVER (PARTITION BY l.lgacode) AS regioncode,  
        FIRST_VALUE(n.region_name) OVER (PARTITION BY l.lgacode) AS region_name  
    FROM visualization.ruwasa_lgas_with_geojson l  
    INNER JOIN visualization.nsmis_household_sanitation_reports_vis n  
        ON n.lgacode = l.nsmislgacode
    
    GROUP BY 
        l.lgacode,  
        l.nsmislgacode,
        l.bemislgacode,
        l.lganame,
        l.districtcode,
        n.regioncode,
        n.region_name,
        l.geojson;
  
  -- SELECT * from visualization.cross_cutting_wash_data_vis limit 100
    --------------------------------------------------------------------------
    --Step 4: Add in the water point functionality data at the LGA level 
    --------------------------------------------------------------------------

    ALTER TABLE visualization.cross_cutting_wash_data_vis
    ADD COLUMN func_rate_new NUMERIC;
    
    UPDATE visualization.cross_cutting_wash_data_vis AS cx
    SET 
      func_rate_new = wp.func_rate_new 
      -- district_name = wp.district_name
    FROM visualization.ruwasa_wps_district AS wp
    WHERE wp.district_code = cx.districtcode;

    --------------------------------------------------------------------------
    -- Step 5: Add in the service level data (derived from infracoverage)
    --------------------------------------------------------------------------

    ALTER TABLE visualization.cross_cutting_wash_data_vis
    ADD COLUMN lga_water_access_level_perc NUMERIC;
    
    --SELECT * from visualization.cross_cutting_wash_data_vis LIMIT 100;
    
    UPDATE visualization.cross_cutting_wash_data_vis AS cx
    SET 
      lga_water_access_level_perc = sl.lga_water_access_level_perc
    FROM visualization.ruwasa_service_level_lga AS sl
    WHERE sl.lgacode = cx.lgacode; 
    
    --------------------------------------------------------------------------
    -- Step 6: Create the LGA level NSMIS data table
    --------------------------------------------------------------------------
  CREATE TABLE visualization.nsmis_household_sanitation_reports_lga AS
  WITH ranked_data AS (
      SELECT DISTINCT ON (lgacode, lga_name, reportdate) 
          lgacode,
          lga_name,
          reportdate,
          regioncode,
          region_name,
          geojson
      FROM visualization.nsmis_household_sanitation_reports_vis
      ORDER BY lgacode, lga_name, reportdate, createdat
  )

  SELECT 
      r.lgacode,
      r.lga_name,
      r.reportdate,
      r.regioncode,
      r.region_name,
      r.geojson,
      
      -- Averages of required percentage variables
      AVG(v.improved_perc_hhs) AS avg_improved_perc_hhs,
      AVG(v.handwashstation_perc_hhs) AS avg_handwashstation_perc_hhs,
      AVG(v.handwashsoap_perc_hhs) AS avg_handwashsoap_perc_hhs

  FROM ranked_data r
  JOIN visualization.nsmis_household_sanitation_reports_vis v
  ON r.lgacode = v.lgacode 
  AND r.lga_name = v.lga_name 
  AND r.reportdate = v.reportdate 

  GROUP BY r.lgacode, r.lga_name, r.reportdate, r.regioncode, r.region_name, r.geojson;
      
    
    
END;
$$;

-- call process

CALL process_cross_cutting_wash_data();

SELECT * from visualization.cross_cutting_wash_data_vis limit 100;
-- they are missing for the Jijis and MCs that is okay as expected 

