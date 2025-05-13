-- ============================================================================
-- process_ruwasa_wp_report: Build and aggregate RUWASA water points report tables
--
-- This procedure creates and populates:
--   * visualization.ruwasa_wp_report_vis (created here)
--   * visualization.ruwasa_wps_district (created here)
--   * visualization.ruwasa_wps_district_quarterly (created here)
-- by transforming, enriching, and aggregating raw RUWASA water points data for reporting and visualization.
--
-- NOTE ON VISUALIZATION SCHEMA:
--   All tables in the visualization schema are derived and must be produced by a procedure.
--   For each visualization table dependency below, ensure the corresponding procedure has been run.
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * public.ruwasa_waterpoints_report (raw, external)
--   * visualization.region_district_lga_names (produced by process_region_district_lga_names)
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure all source tables above are loaded and current (via ETL/import)
--   2. Run process_region_district_lga_names for visualization.region_district_lga_names
--   3. Run this procedure (process_ruwasa_wp_report)
--
-- NOTE: If any dependency is missing or stale, output will be incomplete or incorrect.
--       This script is typically run after all upstream data processing is complete.
-- ============================================================================

-- Create or Replace Procedure to Build and Process RUWASA Water Points Report Tables
CREATE OR REPLACE PROCEDURE public.process_ruwasa_wp_report()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Drop and Create the Main Water Points Visualization Table
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.ruwasa_wp_report_vis';
    EXECUTE 'CREATE TABLE visualization.ruwasa_wp_report_vis AS SELECT * FROM public.ruwasa_waterpoints_report';

    -- Add year_timestamp and populate
    EXECUTE 'ALTER TABLE visualization.ruwasa_wp_report_vis ADD COLUMN year_timestamp TIMESTAMP';
    EXECUTE 'UPDATE visualization.ruwasa_wp_report_vis SET year_timestamp = TO_TIMESTAMP(report_year::TEXT, ''YYYY'')';

    -- Add month_timestamp and populate
    EXECUTE 'ALTER TABLE visualization.ruwasa_wp_report_vis ADD COLUMN month_timestamp TIMESTAMP';
    EXECUTE 'UPDATE visualization.ruwasa_wp_report_vis SET month_timestamp = DATE_TRUNC(''month'', report_datetime)';

    -- Add quarter_timestamp and populate
    EXECUTE 'ALTER TABLE visualization.ruwasa_wp_report_vis ADD COLUMN quarter_timestamp TIMESTAMP';
    EXECUTE 'UPDATE visualization.ruwasa_wp_report_vis SET quarter_timestamp = DATE_TRUNC(''quarter'', report_datetime)';

    -- Add region, district names
    EXECUTE 'ALTER TABLE visualization.ruwasa_wp_report_vis ADD COLUMN region_code VARCHAR, ADD COLUMN region_name VARCHAR, ADD COLUMN district_name VARCHAR(255)';
    EXECUTE '
        UPDATE visualization.ruwasa_wp_report_vis AS wpd
        SET region_code = d.region_code,
            region_name = d.region_name,
            district_name = d.district_name
        FROM visualization.region_district_lga_names AS d
        WHERE wpd.district = d.district_code';
    EXECUTE 'ALTER TABLE visualization.ruwasa_wp_report_vis RENAME COLUMN district TO district_code';

    -- Add functionality dummies
    EXECUTE 'ALTER TABLE visualization.ruwasa_wp_report_vis  
      ADD COLUMN wp_functional INTEGER,  
      ADD COLUMN wp_functional_needs_repair INTEGER,  
      ADD COLUMN wp_non_functional INTEGER,  
      ADD COLUMN wp_abandoned_archived INTEGER,  
      ADD COLUMN wp_inconstruction INTEGER,
      ADD COLUMN wp_func_denominator INTEGER';
    EXECUTE '
        UPDATE visualization.ruwasa_wp_report_vis  
        SET  
            wp_functional = CASE WHEN functionalitystatus = ''functional'' THEN 1 ELSE 0 END,  
            wp_functional_needs_repair = CASE WHEN functionalitystatus = ''functional_need_repair'' THEN 1 ELSE 0 END,  
            wp_non_functional = CASE WHEN functionalitystatus = ''not_functional'' THEN 1 ELSE 0 END,  
            wp_abandoned_archived = CASE WHEN functionalitystatus IN (''abandoned'', ''archived'') THEN 1 ELSE 0 END,  
            wp_inconstruction = CASE WHEN functionalitystatus = ''on_construction'' THEN 1 ELSE 0 END,
            wp_func_denominator = CASE WHEN functionalitystatus IN(''functional'', ''functional_needs_repair'', ''not_functional'') THEN 1 ELSE 0 END';

    -- Add status numeric column
    EXECUTE 'ALTER TABLE visualization.ruwasa_wp_report_vis ADD COLUMN wp_status_numeric NUMERIC';
    EXECUTE '
        UPDATE visualization.ruwasa_wp_report_vis 
        SET wp_status_numeric = 
            CASE 
                WHEN functionalitystatus = ''functional'' THEN 1
                WHEN functionalitystatus = ''functional_needs_repair'' THEN 0.5
                WHEN functionalitystatus IN (''not_functional'', ''abandoned'') THEN 0
                WHEN functionalitystatus IN (''archived'', ''on_construction'') THEN NULL
            END';

    --------------------------------------------------------------------------
    -- Step 2: Create District-Year Aggregation Table
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.ruwasa_wps_district';
    EXECUTE '
        CREATE TABLE visualization.ruwasa_wps_district AS  
        SELECT  
            district_code,  
            year_timestamp, 
            district_name, 
            AVG(wp_abandoned_archived) AS avg_wp_abandoned,  
            SUM(wp_functional) AS sum_wp_functional,  
            AVG(wp_functional_needs_repair) AS avg_wp_functional_needs_repair,  
            AVG(wp_non_functional) AS avg_wp_non_functional,
            AVG(wp_inconstruction) AS avg_in_construction,
            AVG(wp_status_numeric) AS functionality_rate, 
            SUM(wp_func_denominator) AS func_denom,
            FIRST_VALUE(wp.region_code) OVER (PARTITION BY wp.district_code) AS region_code,  
            FIRST_VALUE(wp.region_name) OVER (PARTITION BY wp.district_code) AS region_name  
        FROM visualization.ruwasa_wp_report_vis wp
        GROUP BY district_code, district_name, year_timestamp, region_code, region_name';
    EXECUTE 'ALTER TABLE visualization.ruwasa_wps_district ADD COLUMN func_rate_new NUMERIC';
    EXECUTE '
        UPDATE visualization.ruwasa_wps_district
        SET func_rate_new = 
            CASE 
                WHEN func_denom = 0 THEN NULL
                ELSE sum_wp_functional::NUMERIC / func_denom * 100
            END';

    --------------------------------------------------------------------------
    -- Step 3: Create District-Quarter Aggregation Table
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.ruwasa_wps_district_quarterly';
    EXECUTE '
        CREATE TABLE visualization.ruwasa_wps_district_quarterly AS  
        SELECT  
            district_code,  
            year_timestamp, 
            quarter_timestamp,
            district_name, 
            AVG(wp_abandoned_archived) AS avg_wp_abandoned,  
            SUM(wp_functional) AS sum_wp_functional,  
            AVG(wp_functional_needs_repair) AS avg_wp_functional_needs_repair,  
            AVG(wp_non_functional) AS avg_wp_non_functional,
            AVG(wp_inconstruction) AS avg_in_construction,
            AVG(wp_status_numeric) AS functionality_rate, 
            SUM(wp_func_denominator) AS func_denom,
            FIRST_VALUE(wp.region_code) OVER (PARTITION BY wp.district_code) AS region_code,  
            FIRST_VALUE(wp.region_name) OVER (PARTITION BY wp.district_code) AS region_name  
        FROM visualization.ruwasa_wp_report_vis wp
        GROUP BY district_code, district_name, year_timestamp, quarter_timestamp, region_code, region_name';
    EXECUTE 'ALTER TABLE visualization.ruwasa_wps_district_quarterly ADD COLUMN func_rate_new NUMERIC';
    EXECUTE '
        UPDATE visualization.ruwasa_wps_district_quarterly
        SET func_rate_new = 
            CASE 
                WHEN func_denom = 0 THEN NULL
                ELSE sum_wp_functional::NUMERIC / func_denom
            END';
END;
$$; 