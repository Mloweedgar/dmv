-- ============================================================================
-- process_nsmis_data: Build NSMIS household sanitation visualization tables
--
-- This procedure creates and populates:
--   * visualization.nsmis_household_sanitation_reports_vis (created here)
--   * visualization.nsmis_household_sanitation_lga (created here)
--   * (also updates visualization.dmv_data_quality_flags for data quality issues)
-- by transforming, enriching, and aggregating raw NSMIS data for visualization.
--
-- NOTE ON VISUALIZATION SCHEMA:
--   All tables in the visualization schema are derived and must be produced by a procedure.
--   For each visualization table dependency below, ensure the corresponding procedure has been run.
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * public.nsmis_household_sanitation_reports (raw, external)
--   * public.ruwasa_regions (raw, external)
--   * public.ruwasa_lgas (raw, external)
--   * visualization.ruwasa_lgas_with_geojson (produced by process_ruwasa_lgas_with_geojson)
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure all source tables above are loaded and current (via ETL/import)
--   2. For each visualization.* dependency, run its producing procedure if the table is missing or stale:
--        - process_ruwasa_lgas_with_geojson for visualization.ruwasa_lgas_with_geojson
--   3. Run this procedure (process_nsmis_data)
--
-- NOTE: If any dependency is missing or stale, output will be incomplete or incorrect.
--       This script is typically run after all upstream data processing is complete (recommended schedule: quarterly).
-- ============================================================================

CREATE TABLE IF NOT EXISTS visualization.dmv_data_quality_flags (
  id SERIAL PRIMARY KEY,
  institution TEXT,
  data_type TEXT,           -- e.g., 'householdsanitation'
  error_alert_type TEXT,    -- e.g., 'processing_error', 'unrealistic_values', 'missing_data'
  description TEXT,
  reportdate DATE,
  createdat TIMESTAMP,
  status TEXT,              -- e.g., 'pending', 'resolved'
  resolution_remarks TEXT,
  lastupdatedat TIMESTAMP
);


-- Recommandation: this script can be scheduled  to run quarterly
CREATE OR REPLACE PROCEDURE public.process_nsmis_data()
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

    -- Variables for quality check counts
    v_zero_total_count    INTEGER;
    v_missing_region_count INTEGER;
    v_missing_lga_count    INTEGER;
    
    -- Constants for logging entries
    v_institution TEXT := 'MOH';
    v_data_type   TEXT := 'householdsanitation';
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Create Visualization Table from Raw Data
    --------------------------------------------------------------------------
    BEGIN
      EXECUTE format('DROP TABLE IF EXISTS %I.%I', vis_schema, vis_table_name);
      EXECUTE format('CREATE TABLE %I.%I AS SELECT * FROM %I.%I',
                     vis_schema, vis_table_name, raw_schema, raw_table_name);
    EXCEPTION WHEN OTHERS THEN
      INSERT INTO visualization.dmv_data_quality_flags(
         institution, data_type, error_alert_type, description, reportdate, createdat, status, resolution_remarks, lastupdatedat)
      VALUES (
         v_institution,
         v_data_type,
         'processing_error',
         'Table Creation Error: ' || SQLERRM,
         CURRENT_DATE,
         NOW(),
         'pending',
         '',
         NOW()
      );
      RAISE;
    END;

    --------------------------------------------------------------------------
    -- Step 2: Add Computed Columns to Visualization Table
    --------------------------------------------------------------------------
    BEGIN
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
    EXCEPTION WHEN OTHERS THEN
      INSERT INTO dmv_data_quality_flags(
         institution, data_type, error_alert_type, description, reportdate, createdat, status, resolution_remarks, lastupdatedat)
      VALUES (
         v_institution,
         v_data_type,
         'processing_error',
         'Alter Table Error: ' || SQLERRM,
         CURRENT_DATE,
         NOW(),
         'pending',
         '',
         NOW()
      );
      RAISE;
    END;

    --------------------------------------------------------------------------
    -- Step 3: Compute Derived Fields
    --------------------------------------------------------------------------
    BEGIN
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
          SET improved_perc_hhs = 
            CASE WHEN (improved_count_hhs::DOUBLE PRECISION / NULLIF(totalhouseholds, 0)) > 1 
            THEN 1 
            ELSE (improved_count_hhs::DOUBLE PRECISION / NULLIF(totalhouseholds, 0)) 
            END,

            handwashstation_perc_hhs = 
            CASE WHEN (handwashingstation::DOUBLE PRECISION / NULLIF(totalhouseholds, 0)) > 1 
            THEN 1 
            ELSE (handwashingstation::DOUBLE PRECISION / NULLIF(totalhouseholds, 0)) 
             END,

            handwashsoap_perc_hhs = 
            CASE WHEN (hwfsoapwater::DOUBLE PRECISION / NULLIF(totalhouseholds, 0)) > 1 
            THEN 1 
            ELSE (hwfsoapwater::DOUBLE PRECISION / NULLIF(totalhouseholds, 0))
            END; ', 
          vis_schema, vis_table_name);
    EXCEPTION WHEN OTHERS THEN
      INSERT INTO dmv_data_quality_flags(
         institution, data_type, error_alert_type, description, reportdate, createdat, status, resolution_remarks, lastupdatedat)
      VALUES (
         v_institution,
         v_data_type,
         'processing_error',
         'Derived Fields Computation Error: ' || SQLERRM,
         CURRENT_DATE,
         NOW(),
         'pending',
         '',
         NOW()
      );
      RAISE;
    END;

    -- Data Quality Check: Flag rows with zero totalhouseholds
    EXECUTE format('SELECT COUNT(*) FROM %I.%I WHERE totalhouseholds = 0',
                   vis_schema, vis_table_name)
      INTO v_zero_total_count;
    IF v_zero_total_count > 0 THEN
      INSERT INTO dmv_data_quality_flags(
         institution, data_type, error_alert_type, description, reportdate, createdat, status, resolution_remarks, lastupdatedat)
      VALUES (
         v_institution,
         v_data_type,
         'unrealistic_values',
         'Rows with computed totalhouseholds = 0',
         CURRENT_DATE,
         NOW(),
         'pending',
         'Review raw data for inconsistencies',
         NOW()
      );
    END IF;

    --------------------------------------------------------------------------
    -- Step 4: Enrich Data with Region and LGA Names and a geojson column
    --------------------------------------------------------------------------
    BEGIN
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
    EXCEPTION WHEN OTHERS THEN
      INSERT INTO dmv_data_quality_flags(
         institution, data_type, error_alert_type, description, reportdate, createdat, status, resolution_remarks, lastupdatedat)
      VALUES (
         v_institution,
         v_data_type,
         'processing_error',
         'Data Enrichment Error: ' || SQLERRM,
         CURRENT_DATE,
         NOW(),
         'pending',
         '',
         NOW()
      );
      RAISE;
    END;

    -- QC: Flag missing region or LGA names
    -- description:
    -- error_alert_type: 
    EXECUTE format('SELECT COUNT(*) FROM %I.%I WHERE regioncode IS NOT NULL AND region_name IS NULL',
                   vis_schema, vis_table_name)
      INTO v_missing_region_count;
    IF v_missing_region_count > 0 THEN
      INSERT INTO dmv_data_quality_flags(
         institution, data_type, error_alert_type, description, reportdate, createdat, status, resolution_remarks, lastupdatedat)
      VALUES (
         v_institution,
         v_data_type,
         'missing_data',
         'Rows with non-null regioncode but null region_name',
         CURRENT_DATE,
         NOW(),
         'pending',
         'Verify lookup table for regions',
         NOW()
      );
    END IF;

    EXECUTE format('SELECT COUNT(*) FROM %I.%I WHERE lgacode IS NOT NULL AND lga_name IS NULL',
                   vis_schema, vis_table_name)
      INTO v_missing_lga_count;
    IF v_missing_lga_count > 0 THEN
      INSERT INTO dmv_data_quality_flags(
         institution, data_type, error_alert_type, description, reportdate, createdat, status, resolution_remarks, lastupdatedat)
      VALUES (
         v_institution,
         v_data_type,
         'missing_data',
         'Rows with non-null lgacode but null lga_name',
         CURRENT_DATE,
         NOW(),
         'pending',
         'Verify lookup table for LGAs',
         NOW()
      );
    END IF;

    --------------------------------------------------------------------------
    -- Step 5: Create LGA Summary Table
    --------------------------------------------------------------------------
    BEGIN
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
          vis_schema, lga_summary_name, vis_schema, vis_table_name);
    EXCEPTION WHEN OTHERS THEN
      INSERT INTO dmv_data_quality_flags(
         institution, data_type, error_alert_type, description, reportdate, createdat, status, resolution_remarks, lastupdatedat)
      VALUES (
         v_institution,
         v_data_type,
         'processing_error',
         'LGA Summary Table Creation Error: ' || SQLERRM,
         CURRENT_DATE,
         NOW(),
         'pending',
         '',
         NOW()
      );
      RAISE;
    END;
   
    -- for the sake of visualization, we are dropping the observations where the values are null because otherwise it won't generate the bars
    DELETE FROM visualization.nsmis_household_sanitation_lga
    WHERE 
        avg_improved_perc_hhs IS NULL 
        OR avg_handwashsoap_perc_hhs IS NULL
        OR avg_handwashstation_perc_hhs IS NULL;
END;
$$;
