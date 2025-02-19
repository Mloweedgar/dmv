CREATE OR REPLACE FUNCTION process_nsmis_data_trigger_fn()
RETURNS trigger AS $$
DECLARE
    -- Raw table information
    raw_schema      TEXT := 'public';
    raw_table_name  TEXT := 'nsmis_household_sanitation_reports';
    
    -- Visualization (intermediate/final) table information
    vis_schema      TEXT := 'visualization';
    vis_table_name  TEXT := 'nsmis_household_sanitation_reports_vis';  -- Intermediate table
    final_vis_name  TEXT := 'nsmis_household_sanitation_reports_v';    -- Temporary table for reordering, later renamed
    
    -- LGA summary table information
    lga_summary_name TEXT := 'nsmis_household_sanitation_reports_lga';  -- Intermediate summary table
    final_lga_name   TEXT := 'nsmis_household_sanitation_lga';          -- Final LGA summary table
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Create Intermediate Visualization Table from Raw Data
    --------------------------------------------------------------------------
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', vis_schema, vis_table_name);
    EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I', vis_schema, vis_table_name, raw_schema, raw_table_name);

    --------------------------------------------------------------------------
    -- Step 2: Add Computed Columns to the Intermediate Visualization Table
    --------------------------------------------------------------------------
    EXECUTE format('
        ALTER TABLE %I.%I 
        ADD COLUMN IF NOT EXISTS totalhouseholds INTEGER,
        ADD COLUMN IF NOT EXISTS improved_count_hhs INTEGER,
        ADD COLUMN IF NOT EXISTS improved_perc_hhs NUMERIC,
        ADD COLUMN IF NOT EXISTS handwashstation_perc_hhs NUMERIC,
        ADD COLUMN IF NOT EXISTS handwashsoap_perc_hhs NUMERIC,
        ADD COLUMN IF NOT EXISTS region_name VARCHAR(100),
        ADD COLUMN IF NOT EXISTS lga_name VARCHAR(100),
        ADD COLUMN IF NOT EXISTS geojson VARCHAR',
        vis_schema, vis_table_name);

    --------------------------------------------------------------------------
    -- Step 3: Compute Sanitation Statistics in the Intermediate Table
    --------------------------------------------------------------------------
    EXECUTE format('
        UPDATE %I.%I 
        SET totalhouseholds = COALESCE(toilettypea, 0) + COALESCE(toilettypeb, 0) + 
                              COALESCE(toilettypec, 0) + COALESCE(toilettyped, 0) + 
                              COALESCE(toilettypee, 0) + COALESCE(toilettypef, 0) + 
                              COALESCE(toilettypex, 0),
            improved_count_hhs = COALESCE(toilettypeb, 0) + COALESCE(toilettypec, 0) + 
                                 COALESCE(toilettyped, 0) + COALESCE(toilettypee, 0),
            improved_perc_hhs = NULLIF(improved_count_hhs::DOUBLE PRECISION, 0) / NULLIF(totalhouseholds, 0),
            handwashstation_perc_hhs = NULLIF(handwashingstation::DOUBLE PRECISION, 0) / NULLIF(totalhouseholds, 0),
            handwashsoap_perc_hhs = NULLIF(hwfsoapwater::DOUBLE PRECISION, 0) / NULLIF(totalhouseholds, 0)',
        vis_schema, vis_table_name);

    --------------------------------------------------------------------------
    -- Step 4: Enrich Data with Region and LGA Names
    --------------------------------------------------------------------------
    EXECUTE format('
        UPDATE %I.%I AS target
        SET region_name = source.name
        FROM public.ruwasa_regions AS source
        WHERE target.regioncode = source.nsmisregioncode',
        vis_schema, vis_table_name);

    EXECUTE format('
        UPDATE %I.%I AS target
        SET lga_name = source.lganame
        FROM public.ruwasa_lgas AS source
        WHERE target.lgacode = source.nsmislgacode',
        vis_schema, vis_table_name);
        


    --------------------------------------------------------------------------
    -- Step 5: Attach GeoJSON Data for Spatial Analysis
    --------------------------------------------------------------------------
    EXECUTE format('
        UPDATE %I.%I AS target
        SET geojson = source.geojson
        FROM public.tz_lgas AS source
        WHERE target."lga_name" = source."Authority"',
        vis_schema, vis_table_name);

    --------------------------------------------------------------------------
    -- Step 6: Create Final Visualization Table with Reordered Columns
    --------------------------------------------------------------------------
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', vis_schema, final_vis_name);
    EXECUTE format('
        CREATE TABLE %I.%I AS
        SELECT sysid, reportdate, regioncode, region_name, lgacode, lga_name, 
               wardcode, wardname, villagecode, villagename, 
               toilettypea, toilettypeb, toilettypec, toilettyped, toilettypee, 
               toilettypef, toilettypex, handwashingstation, hwfsoapwater, 
               createdat, totalhouseholds, improved_count_hhs, improved_perc_hhs, 
               handwashstation_perc_hhs, handwashsoap_perc_hhs
        FROM %I.%I',
        vis_schema, final_vis_name, vis_schema, vis_table_name);

    EXECUTE format('DROP TABLE IF EXISTS %I.%I', vis_schema, vis_table_name);
    EXECUTE format('ALTER TABLE %I.%I RENAME TO %I', vis_schema, final_vis_name, 'nsmis_household_sanitation_reports_vis');

    --------------------------------------------------------------------------
    -- Step 7: Create Intermediate LGA Summary Table (for aggregation)
    -- Now reference the final visualization table using its schema-qualified name.
    --------------------------------------------------------------------------
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', vis_schema, lga_summary_name);
    EXECUTE format('
        CREATE TABLE %I.%I AS
        WITH ranked_data AS (
            SELECT DISTINCT ON (lgacode, lga_name, reportdate)
                   lgacode, lga_name, reportdate, regioncode, region_name
            FROM %I.%I
            ORDER BY lgacode, lga_name, reportdate, createdat
        )
        SELECT 
            r.lgacode, r.lga_name, r.reportdate, r.regioncode, r.region_name,
            AVG(v.improved_perc_hhs) AS avg_improved_perc_hhs,
            AVG(v.handwashstation_perc_hhs) AS avg_handwashstation_perc_hhs,
            AVG(v.handwashsoap_perc_hhs) AS avg_handwashsoap_perc_hhs
        FROM ranked_data r
        JOIN %I.%I v
          ON r.lgacode = v.lgacode 
         AND r.lga_name = v.lga_name 
         AND r.reportdate = v.reportdate
        GROUP BY r.lgacode, r.lga_name, r.reportdate, r.regioncode, r.region_name',
        vis_schema, lga_summary_name, vis_schema, 'nsmis_household_sanitation_reports_vis', vis_schema, 'nsmis_household_sanitation_reports_vis');

    --------------------------------------------------------------------------
    -- Step 8: Create Final LGA Summary Table
    --------------------------------------------------------------------------
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', vis_schema, final_lga_name);
    EXECUTE format('
        CREATE TABLE %I.%I AS
        SELECT regioncode, region_name, lgacode, lga_name, reportdate, 
               avg_improved_perc_hhs, avg_handwashsoap_perc_hhs, avg_handwashstation_perc_hhs
        FROM %I.%I',
        vis_schema, final_lga_name, vis_schema, lga_summary_name);

    EXECUTE format('DROP TABLE IF EXISTS %I.%I', vis_schema, lga_summary_name);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


DROP TRIGGER IF EXISTS process_nsmis_data_trigger ON public.nsmis_household_sanitation_reports;

CREATE TRIGGER process_nsmis_data_trigger
AFTER INSERT OR UPDATE OR DELETE
ON public.nsmis_household_sanitation_reports
FOR EACH STATEMENT
EXECUTE FUNCTION process_nsmis_data_trigger_fn();

