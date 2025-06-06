-- ============================================================================
-- process_region_district_lga_names: Build Region-District-LGA lookup table
--
-- This procedure creates (or refreshes) the canonical lookup table:
--   * visualization.region_district_lga_names
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * public.ruwasa_lgas        -- raw LGA list with codes
--   * public.ruwasa_districts   -- raw district list with codes
--   * public.ruwasa_regions     -- raw region list with codes
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure the three source tables above are loaded and current.
--   2. Run this procedure (process_region_district_lga_names).
--
-- NOTE: Down-stream procedures rely on this lookup; run it before any step
--       that references visualization.region_district_lga_names.
-- ============================================================================

CREATE OR REPLACE PROCEDURE process_region_district_lga_names()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Replace existing lookup table, if present
    --------------------------------------------------------------------------
    DROP TABLE IF EXISTS visualization.region_district_lga_names;

    --------------------------------------------------------------------------
    -- Step 2: Build the Region-District-LGA lookup (includes GeoJSON in source)
    --------------------------------------------------------------------------
    CREATE TABLE visualization.region_district_lga_names AS
    SELECT
        r.name          AS region_name,
        d.districtname  AS district_name,
        l.lganame       AS lga_name,
        r.code          AS region_code,
        r.bemisregioncode,
        r.nsmisregioncode,
        d.districtcode  AS district_code,
        l.lgacode       AS lga_code,
        l.bemislgacode,
        l.nsmislgacode
    FROM   public.ruwasa_lgas      l
    JOIN   public.ruwasa_districts d ON l.districtcode = d.districtcode
    JOIN   public.ruwasa_regions   r ON d.regioncode   = r.code;

END;
$$;
