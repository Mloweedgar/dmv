-- ============================================================================
-- process_region_district_lga_names: Build region, district, and LGA names lookup table
--
-- This procedure creates and populates:
--   * visualization.region_district_lga_names (created here)
-- by joining spatial and administrative reference data for use in downstream reporting and visualization.
--
-- NOTE ON VISUALIZATION SCHEMA:
--   All tables in the visualization schema are derived and must be produced by a procedure.
--   For each visualization table dependency below, ensure the corresponding procedure has been run.
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * visualization.ruwasa_lgas_with_geojson (produced by process_ruwasa_lgas_with_geojson)
--   * public.ruwasa_districts (raw, external)
--   * public.ruwasa_regions (raw, external)
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure all source tables above are loaded and current (via ETL/import)
--   2. Run process_ruwasa_lgas_with_geojson for visualization.ruwasa_lgas_with_geojson
--   3. Run this procedure (process_region_district_lga_names)
--
-- NOTE: If any dependency is missing or stale, output will be incomplete or incorrect.
--       This script is typically run after spatial or administrative reference data is updated.
-- ============================================================================
CREATE OR REPLACE PROCEDURE public.process_region_district_lga_names()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1.1: Drop the Existing Table if It Exists
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.region_district_lga_names';

    --------------------------------------------------------------------------
    -- Step 1.2: Create the Table by Joining Regions, Districts, and LGAs
    --------------------------------------------------------------------------
    EXECUTE '
    CREATE TABLE visualization.region_district_lga_names AS 
    SELECT 
        r.name AS region_name, 
        d.districtname AS district_name, 
        l.lganame AS lga_name,
        r.code AS region_code, 
        r.bemisregioncode, 
        r.nsmisregioncode,
        d.districtcode AS district_code,
        l.lgacode AS lga_code, 
        l.bemislgacode, 
        l.nsmislgacode,
        l.geojson
    FROM visualization.ruwasa_lgas_with_geojson l 
    JOIN public.ruwasa_districts d 
        ON l.districtcode = d.districtcode
    JOIN public.ruwasa_regions r 
        ON d.regioncode = r.code
    ';

END;
$$;

-- QC: I just want to check on lines 51 and 52 - are you joining lgas with districts? as lgas can only be joined with LGAs and likewise with districts