-- Create or Replace Procedure to Build RUWASA District Infracoverage Table
-- This procedure creates and enriches the visualization.ruwasa_district_infracoverage table
CREATE OR REPLACE PROCEDURE process_ruwasa_district_infracoverage()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Drop Existing Table if It Exists
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.ruwasa_district_infracoverage';

    --------------------------------------------------------------------------
    -- Step 2: Create the Table with Average infracoverage per District
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

    --------------------------------------------------------------------------
    -- Step 3: Add Region and District Name Columns
    --------------------------------------------------------------------------
    EXECUTE '
    ALTER TABLE visualization.ruwasa_district_infracoverage
      ADD COLUMN region_code VARCHAR,
      ADD COLUMN region_name VARCHAR,
      ADD COLUMN district_name VARCHAR(255)
    ';

    --------------------------------------------------------------------------
    -- Step 4: Update Region and District Names
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
    -- Step 5: Rename Column district to district_code
    --------------------------------------------------------------------------
    EXECUTE '
    ALTER TABLE visualization.ruwasa_district_infracoverage
    RENAME COLUMN district TO district_code
    ';

END;
$$; 