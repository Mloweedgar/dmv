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
    EXECUTE '
        CREATE TABLE visualization.ruwasa_wp_report_vis AS
        SELECT
            wp.report_datetime,
            wp.region,
            wp.district AS district_code,
            wp.village,
            wp.dpid,
            wp.dpname,
            wp.wps,
            wp.dpcode,
            wp.latitude,
            wp.longitude,
            wp.populationserved,
            wp.yoc,
            wp.meternumber,
            wp.scheme,
            wp.schemetype,
            wp.servicearea,
            wp.paymenttype,
            wp.functionalitystatus,
            wp.organisation,
            wp.totalavailablegaps,
            wp.completeness,
            wp.ward,
            wp.servedbyruwasa,
            wp.lga,
            wp.institutioncategoryid,
            wp.institutionareaid,
            wp.sysid,
            wp.report_year,
            wp.quarter,
            wp.createdat,
            -- Derived columns:
            TO_TIMESTAMP(wp.report_year, ''YYYY'') AS year_timestamp,
            DATE_TRUNC(''month'', wp.report_datetime) AS month_timestamp,
            DATE_TRUNC(''quarter'', wp.report_datetime) AS quarter_timestamp,
            CASE WHEN wp.functionalitystatus = ''functional'' THEN 1 ELSE 0 END AS wp_functional,
            CASE WHEN wp.functionalitystatus = ''functional_need_repair'' THEN 1 ELSE 0 END AS wp_functional_needs_repair,
            CASE WHEN wp.functionalitystatus = ''not_functional'' THEN 1 ELSE 0 END AS wp_non_functional,
            CASE WHEN wp.functionalitystatus IN (''abandoned'', ''archived'') THEN 1 ELSE 0 END AS wp_abandoned_archived,
            CASE WHEN wp.functionalitystatus = ''on_construction'' THEN 1 ELSE 0 END AS wp_inconstruction,
            CASE WHEN wp.functionalitystatus IN (''functional'', ''functional_need_repair'', ''not_functional'') THEN 1 ELSE 0 END AS wp_func_denominator,
            CASE
                WHEN wp.functionalitystatus = ''functional'' THEN 1
                WHEN wp.functionalitystatus = ''functional_need_repair'' THEN 0.5
                WHEN wp.functionalitystatus IN (''not_functional'', ''abandoned'') THEN 0
                WHEN wp.functionalitystatus IN (''archived'', ''on_construction'') THEN NULL
            END AS wp_status_numeric
        FROM public.ruwasa_waterpoints_report wp
    ';
    
    -- QC: The below is a soft quality check that ultimately wants to be more of an output with summary statistics each time this script is run. Not necessarily an error
         CREATE TABLE quality_checks.temp_functionality_summary AS
            SELECT 
                functionalitystatus, 
                COUNT(*) AS count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
            FROM visualization.ruwasa_wp_report_vis
            GROUP BY functionalitystatus
            ORDER BY count DESC;
    
    -- Add region/district columns
    EXECUTE 'ALTER TABLE visualization.ruwasa_wp_report_vis ADD COLUMN region_code VARCHAR, ADD COLUMN region_name VARCHAR, ADD COLUMN district_name VARCHAR';
    -- Populate region/district columns with a single update using a join
    EXECUTE '
        UPDATE visualization.ruwasa_wp_report_vis vis
        SET region_code = d.region_code,
            region_name = d.region_name,
            district_name = d.district_name
        FROM visualization.region_district_lga_names d
        WHERE vis.district_code = d.district_code
    ';

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

    -- QC: check if there are districts and years with blank values for all functionality statuses and print them out i.e. district name, year where sum_wp_functional == BLANK

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