CREATE OR REPLACE PROCEDURE process_nsmis_data()
LANGUAGE plpgsql
AS $$
DECLARE
    -- Raw table information
    raw_schema      TEXT := 'public';
    raw_table_name  TEXT := 'nsmis_household_sanitation_reports';
    
    -- Output Visualization tables
    vis_schema      TEXT := 'visualization';
    vis_table_name  TEXT := 'nsmis_household_sanitation_reports_vis';
    lga_summary_name TEXT := 'nsmis_household_sanitation_lga';
    
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Create  Visualization Table from Raw Data
    --------------------------------------------------------------------------
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', vis_schema, vis_table_name);
    EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I', vis_schema, vis_table_name, raw_schema, raw_table_name);

    --------------------------------------------------------------------------
    -- Step 2: Add Computed Columns to Visualization Table
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
    -- Step 3: Compute Derived Fields
    --------------------------------------------------------------------------
    EXECUTE format('
        UPDATE %I.%I 
        SET totalhouseholds = COALESCE(toilettypea, 0) + COALESCE(toilettypeb, 0) + 
                              COALESCE(toilettypec, 0) + COALESCE(toilettyped, 0) + 
                              COALESCE(toilettypee, 0) + COALESCE(toilettypef, 0) + 
                              COALESCE(toilettypex, 0)',
        vis_schema, vis_table_name);

    EXECUTE format('
        UPDATE %I.%I 
        SET improved_count_hhs = COALESCE(toilettypeb, 0) + COALESCE(toilettypec, 0) + 
                                 COALESCE(toilettyped, 0) + COALESCE(toilettypee, 0)',
        vis_schema, vis_table_name);

    EXECUTE format('
        UPDATE %I.%I 
        SET improved_perc_hhs = improved_count_hhs::DOUBLE PRECISION / NULLIF(totalhouseholds, 0),
            handwashstation_perc_hhs = handwashingstation::DOUBLE PRECISION / NULLIF(totalhouseholds, 0),
            handwashsoap_perc_hhs = hwfsoapwater::DOUBLE PRECISION / NULLIF(totalhouseholds, 0)',
        vis_schema, vis_table_name);

    --------------------------------------------------------------------------
    -- Step 4: Enrich Data with Region and LGA Names and a geojson column
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

    EXECUTE format('
        UPDATE %I.%I AS target
        SET geojson = source.geojson
        FROM visualization.ruwasa_lgas_with_geojson AS source
        WHERE target.lgacode = source.nsmislgacode',
        vis_schema, vis_table_name);

    --------------------------------------------------------------------------
    -- Step 5: Create  LGA Summary Table
    --------------------------------------------------------------------------
    EXECUTE format('DROP TABLE IF EXISTS %I.%I', vis_schema, lga_summary_name);
    EXECUTE format('
        CREATE TABLE %I.%I AS
        SELECT lgacode, lga_name, reportdate, regioncode, region_name,
               AVG(improved_perc_hhs) AS avg_improved_perc_hhs,
               AVG(handwashstation_perc_hhs) AS avg_handwashstation_perc_hhs,
               AVG(handwashsoap_perc_hhs) AS avg_handwashsoap_perc_hhs,
               geojson
        FROM %I.%I
        GROUP BY lgacode, lga_name, reportdate, regioncode, region_name, geojson',
        vis_schema, lga_summary_name, vis_schema, 'nsmis_household_sanitation_reports_vis');

END;
$$;
