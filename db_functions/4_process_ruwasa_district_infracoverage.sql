-- ============================================================================
-- process_ruwasa_district_infracoverage: Build RUWASA district infracoverage tables (yearly and most recent)
--
-- This procedure creates and populates:
--   * visualization.ruwasa_district_infracoverage_yearly (yearly averages, for big number/evolution charts)
--   * visualization.ruwasa_district_infracoverage (most recent, for cross-cutting charts)
-- by aggregating and enriching RUWASA coverage data at the district level for reporting and visualization.
--
-- NOTE ON VISUALIZATION SCHEMA:
--   All tables in the visualization schema are derived and must be produced by a procedure.
--   For each visualization table dependency below, ensure the corresponding procedure has been run.
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * foreign_schema_ruwasa_rsdms.ruwasa_reports_coverage (raw, external)
--   * visualization.region_district_lga_names (produced by process_region_district_lga_names)
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure all source tables above are loaded and current (via ETL/import)
--   2. Run process_region_district_lga_names for visualization.region_district_lga_names
--   3. Run this procedure (process_ruwasa_district_infracoverage)
--
-- NOTE: If any dependency is missing or stale, output will be incomplete or incorrect.
--       This script is typically run after all upstream data processing is complete.
-- ============================================================================

-- Create or Replace Procedure to Build RUWASA District Infracoverage Tables
-- This procedure creates and enriches the visualization.ruwasa_district_infracoverage_yearly and visualization.ruwasa_district_infracoverage tables
CREATE OR REPLACE PROCEDURE public.process_ruwasa_district_infracoverage()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Drop Existing Tables if They Exist
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.ruwasa_district_infracoverage_yearly';
    EXECUTE 'DROP TABLE IF EXISTS visualization.ruwasa_district_infracoverage';

    --------------------------------------------------------------------------
    -- Step 2: Create Yearly Infracoverage Table (for big number/evolution charts)
    --------------------------------------------------------------------------
    EXECUTE '
    CREATE TABLE visualization.ruwasa_district_infracoverage_yearly AS
    SELECT 
        district,
        EXTRACT(YEAR FROM report_datetime) AS year,
        AVG(infracoverage) AS avg_infracoverage
    FROM foreign_schema_ruwasa_rsdms.ruwasa_reports_coverage
    GROUP BY district, EXTRACT(YEAR FROM report_datetime)
    ORDER BY district, year
    ';

    --------------------------------------------------------------------------
    -- Step 3: Add Region and District Name Columns to Yearly Table
    --------------------------------------------------------------------------
    EXECUTE '
    ALTER TABLE visualization.ruwasa_district_infracoverage_yearly
      ADD COLUMN region_code VARCHAR,
      ADD COLUMN region_name VARCHAR,
      ADD COLUMN district_name VARCHAR(255)
    ';

    --------------------------------------------------------------------------
    -- Step 4: Update Region and District Names in Yearly Table
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.ruwasa_district_infracoverage_yearly AS y
      SET 
        district_name = n.district_name,
        region_name = n.region_name,
        region_code = n.region_code
    FROM visualization.region_district_lga_names AS n
    WHERE y.district = n.district_code
    ';

    --------------------------------------------------------------------------
    -- Step 5: Rename Column district to district_code in Yearly Table
    --------------------------------------------------------------------------
    EXECUTE '
    ALTER TABLE visualization.ruwasa_district_infracoverage_yearly
    RENAME COLUMN district TO district_code
    ';

    --------------------------------------------------------------------------
    -- Step 6: Add year_timestamp Column and Populate as Timestamp in Yearly Table
    --------------------------------------------------------------------------
    EXECUTE '
    ALTER TABLE visualization.ruwasa_district_infracoverage_yearly ADD COLUMN year_timestamp TIMESTAMP
    ';
    EXECUTE '
    UPDATE visualization.ruwasa_district_infracoverage_yearly
    SET year_timestamp = TO_TIMESTAMP(year::TEXT, ''YYYY'')
    ';

    --------------------------------------------------------------------------
    -- Step 7: Create Most Recent Infracoverage Table (for cross-cutting charts)
    --------------------------------------------------------------------------

  
    EXECUTE '
    CREATE TABLE visualization.ruwasa_district_infracoverage AS
    SELECT district, AVG(infracoverage) AS avg_infracoverage
    FROM foreign_schema_ruwasa_rsdms.ruwasa_reports_coverage
    WHERE report_datetime::DATE = (
        SELECT MAX(report_datetime::DATE)
        FROM foreign_schema_ruwasa_rsdms.ruwasa_reports_coverage
    ) AND infracoverage > 0 
    GROUP BY district
    ';

    -- the above code creates a version with only the most recent year - Instead we need one version with yearly and one version with most recent 
    -- refer to this script to change it https://rsdms.ruwasa.go.tz:8066/sqllab/?savedQueryId=53 

    --- note to edgar to modify the code above to do this process instead 

    --------------------------------------------------------------------------
    -- Step 8: Add Region and District Name Columns to Most Recent Table
    --------------------------------------------------------------------------
    EXECUTE '
    ALTER TABLE visualization.ruwasa_district_infracoverage
      ADD COLUMN region_code VARCHAR,
      ADD COLUMN region_name VARCHAR,
      ADD COLUMN district_name VARCHAR(255)
    ';

    --------------------------------------------------------------------------
    -- Step 9: Update Region and District Names in Most Recent Table
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.ruwasa_district_infracoverage AS d
      SET 
        district_name = n.district_name,
        region_name = n.region_name,
        region_code = n.region_code
    FROM visualization.region_district_lga_names AS n
    WHERE d.district = n.district_code
    ';

    --------------------------------------------------------------------------
    -- Step 10: Rename Column district to district_code in Most Recent Table
    --------------------------------------------------------------------------
    EXECUTE '
    ALTER TABLE visualization.ruwasa_district_infracoverage
    RENAME COLUMN district TO district_code
    ';

END;
$$; 