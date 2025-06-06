-- ============================================================================
-- process_region_district_lga_lookup: Build lightweight region-district-LGA lookup
--
-- This procedure creates and populates:
--   * visualization.region_district_lga_names_without_geojson
--
-- It is a geometry-free variant of `visualization.region_district_lga_names`,
-- intended for fast, non-spatial joins in reporting queries.
--
-- DEPENDENCY (must exist and be current BEFORE running):
--   * visualization.region_district_lga_names   -- from process_region_district_lga_names
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Run/refresh process_region_district_lga_names.
--   2. Run this procedure (process_region_district_lga_lookup).
-- ============================================================================

CREATE OR REPLACE PROCEDURE public.process_region_district_lga_lookup()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Replace existing lookup table, if any
    --------------------------------------------------------------------------
    DROP TABLE IF EXISTS visualization.region_district_lga_names_without_geojson;

    --------------------------------------------------------------------------
    -- Step 2: Create the lookup table without GeoJSON
    --------------------------------------------------------------------------
    CREATE TABLE visualization.region_district_lga_names_without_geojson AS
    SELECT
        region_name,
        district_name,
        lga_name       AS lganame,
        region_code,
        bemisregioncode,
        nsmisregioncode,
        district_code,
        lga_code,
        bemislgacode,
        nsmislgacode
    FROM visualization.region_district_lga_names;

END;
$$;
