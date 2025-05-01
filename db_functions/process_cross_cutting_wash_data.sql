
--  Create a Procedure to Populate the Cross-Cutting Wash Data Visualization Table
-- Note: this script should be run after running other scripts for processing nsmis,bemis and rsdms data. 
-- so that the tables needed for cross cutting data processing will be ready
-- Recommandation: this script can be scheduled to run monthly

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


    --------------------------------------------------------------------------
    -- Step 7: Generate summary NSMIS household sanitation data at the LGA level
    --------------------------------------------------------------------------
  CREATE TABLE visualization.nsmis_household_sanitation_lga AS
  SELECT
    regioncode,
    region_name,
    lgacode,
    lga_name,
    reportdate,
    avg_improved_perc_hhs,
    avg_handwashsoap_perc_hhs,
    avg_handwashstation_perc_hhs

    FROM visualization.nsmis_household_sanitation_reports_lga;
      
    
    
END;
$$;

-- call process

CALL process_cross_cutting_wash_data();

SELECT * from visualization.cross_cutting_wash_data_vis limit 100;
-- they are missing for the Jijis and MCs that is okay as expected 

