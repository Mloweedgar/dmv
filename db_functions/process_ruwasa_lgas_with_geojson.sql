-- ============================================================================
-- process_ruwasa_lgas_with_geojson: Build RUWASA LGAs table with GeoJSON shapes
--
-- This procedure creates and populates:
--   * visualization.ruwasa_lgas_with_geojson (created here)
-- by copying public.ruwasa_lgas and joining with public.tz_lgas to add GeoJSON shapes for each LGA.
--
-- NOTE ON VISUALIZATION SCHEMA:
--   All tables in the visualization schema are derived and must be produced by a procedure.
--   For each visualization table dependency below, ensure the corresponding procedure has been run.
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * public.ruwasa_lgas (raw, external)
--   * public.tz_lgas (raw, external)
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure all source tables above are loaded and current (via ETL/import)
--   2. Run this procedure (process_ruwasa_lgas_with_geojson)
--
-- NOTE: If any dependency is missing or stale, output will be incomplete or incorrect.
--       This script is typically run after spatial or administrative reference data is updated.
-- ============================================================================

CREATE OR REPLACE PROCEDURE public.process_ruwasa_lgas_with_geojson()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Step 1: Drop the table if it exists
    DROP TABLE IF EXISTS visualization.ruwasa_lgas_with_geojson;

    -- Step 2: Create the table from public.ruwasa_lgas
    CREATE TABLE visualization.ruwasa_lgas_with_geojson AS
    SELECT * FROM public.ruwasa_lgas;

    -- Step 3: Add the geojson column
    ALTER TABLE visualization.ruwasa_lgas_with_geojson 
      ADD COLUMN geojson VARCHAR;

    -- Step 4: Update geojson values by joining with public.tz_lgas
    UPDATE visualization.ruwasa_lgas_with_geojson AS target
    SET geojson = source.geojson
    FROM public.tz_lgas AS source
    WHERE target.lganame = source."Authority";
END;
$$; 