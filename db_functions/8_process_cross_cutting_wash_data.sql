-- ============================================================================
-- process_cross_cutting_wash_data: Build cross-sector WASH visualization tables
--
-- This procedure creates and populates:
--   * visualization.cross_cutting_wash_data_vis (main output, created here)
--   * visualization.ruwasa_service_level_lga (intermediate, created here)
--   * visualization.nsmis_household_sanitation_reports_lga (summary, created here)
-- by aggregating and joining data from NSMIS, RUWASA, and spatial reference tables.
--
-- NOTE ON VISUALIZATION SCHEMA:
--   All tables in the visualization schema are derived and must be produced by a procedure.
--   For each visualization table dependency below, ensure the corresponding procedure has been run.
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * foreign_schema_ruwasa_rsdms.ruwasa_villages (raw, external)
--   * visualization.ruwasa_lgas_with_geojson (produced by process_ruwasa_lgas_with_geojson)
--   * visualization.nsmis_household_sanitation_reports_vis (produced by process_nsmis_data)
--   * visualization.ruwasa_wps_district (produced by process_ruwasa_wp_report)
--   * public.ruwasa_districts (raw, external)
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure all source tables above are loaded and current (via ETL/import)
--   2. For each visualization.* dependency, run its producing procedure if the table is missing or stale:
--        - process_ruwasa_lgas_with_geojson for visualization.ruwasa_lgas_with_geojson
--        - process_nsmis_data for visualization.nsmis_household_sanitation_reports_vis
--        - process_ruwasa_wp_report for visualization.ruwasa_wps_district
--        - (and so on for other visualization.* dependencies)
--   3. Run this procedure (process_cross_cutting_wash_data)
--
-- NOTE: If any dependency is missing or stale, output will be incomplete or incorrect.
--       This script is typically run after all upstream data processing is complete (e.g., after NSMIS, BEMIS, and RSDMS data are processed).
--       Recommended schedule: monthly.
-- ============================================================================


CREATE OR REPLACE PROCEDURE public.process_cross_cutting_wash_data()
LANGUAGE plpgsql
AS $$
BEGIN
/* ──────────────────────────────────────────────────────────
   1.  Temp helpers: aggregate big source tables first
─────────────────────────────────────────────────────────── */
CREATE TEMP TABLE _service_level_lga ON COMMIT DROP AS
SELECT  lgacode,
        AVG(infracoverage)                                AS lga_water_access_level_perc,
        SUM( (status=1 AND isvillage)::int )              AS villages_in_lga,
        SUM( (status=1 AND isvillage AND servicetype<>'no_service')::int ) AS villages_with_service,
        SUM( (status=1 AND isvillage AND servicetype='no_service')::int )  AS villages_no_service,
        SUM( (status=1 AND isvillage AND NOT servedbyruwasa)::int )        AS served_by_wssa
FROM    foreign_schema_ruwasa_rsdms.ruwasa_villages
GROUP   BY lgacode;

CREATE TEMP TABLE _hh_agg ON COMMIT DROP AS
SELECT  lgacode,
        MIN(regioncode)                     AS regioncode,
        MIN(region_name)                    AS region_name,
        AVG(improved_perc_hhs::numeric)     AS avg_imp_hh,
        AVG(handwashstation_perc_hhs::numeric) AS avg_hw_station,
        AVG(handwashsoap_perc_hhs::numeric)    AS avg_hw_soap
FROM    visualization.nsmis_household_sanitation_reports_vis
GROUP   BY lgacode;

CREATE TEMP TABLE _school_agg ON COMMIT DROP AS
SELECT  lga_code                            AS bemislgacode,
        AVG(improved_water_source::numeric) AS avg_water_src,
        AVG(improved_toilet_type::numeric)  AS avg_toilet_type
FROM    visualization.bemis_school_comb_vis
GROUP   BY lga_code;

/* ──────────────────────────────────────────────────────────
   2.  Build cross-cutting table in a staging name
─────────────────────────────────────────────────────────── */
DROP TABLE IF EXISTS visualization.cross_cutting_wash_data_vis_stage;
CREATE TABLE     visualization.cross_cutting_wash_data_vis_stage AS
SELECT  l.lgacode,
        l.nsmislgacode,
        l.bemislgacode,
        l.lganame,
        l.districtcode,
        d.districtname as district_name,
        l.geojson,

        ROUND(h.avg_imp_hh     *100,1)  AS lga_improved_san_perc_hhs,
        ROUND(h.avg_hw_station *100,1)  AS lga_handwashstation_perc_hhs,
        ROUND(h.avg_hw_soap    *100,1)  AS lga_handwashsoap_perc_hhs,

        h.regioncode,
        h.region_name,

        ROUND(s.avg_water_src  *100,1)  AS lga_school_improved_water_perc,
        ROUND(s.avg_toilet_type*100,1)  AS lga_school_improved_toilet_type_perc
FROM        visualization.ruwasa_lgas_with_geojson l
LEFT JOIN   _hh_agg      h ON h.lgacode      = l.nsmislgacode
LEFT JOIN   _school_agg  s ON s.bemislgacode = l.bemislgacode
LEFT JOIN   public.ruwasa_districts d ON d.districtcode = l.districtcode;

/* add empty columns to be updated */
ALTER TABLE visualization.cross_cutting_wash_data_vis_stage
  ADD COLUMN func_rate_new               numeric,
  ADD COLUMN lga_water_access_level_perc numeric;

/* 2a – water-point functionality */
UPDATE visualization.cross_cutting_wash_data_vis_stage cx
SET    func_rate_new = wp.func_rate_new
FROM   visualization.ruwasa_wps_district wp
WHERE  wp.district_code = cx.districtcode;

/* 2b – village service-level percentage */
UPDATE visualization.cross_cutting_wash_data_vis_stage cx
SET    lga_water_access_level_perc = sl.lga_water_access_level_perc
FROM   _service_level_lga sl
WHERE  sl.lgacode = cx.lgacode;

/* swap stage → production */
DROP TABLE IF EXISTS visualization.cross_cutting_wash_data_vis;
ALTER TABLE visualization.cross_cutting_wash_data_vis_stage
        RENAME TO cross_cutting_wash_data_vis;

/* ──────────────────────────────────────────────────────────
   3.  Household-only LGA summary (matches original columns)
─────────────────────────────────────────────────────────── */
-- TODO: move this to nsmis script
DROP TABLE IF EXISTS visualization.nsmis_household_sanitation_reports_lga;
CREATE TABLE visualization.nsmis_household_sanitation_reports_lga AS
WITH ranked_data AS (
    /* pick one row per (LGA, reportdate) keeping geojson & region labels */
    SELECT DISTINCT ON (lgacode, lga_name, reportdate)
           lgacode, lga_name, reportdate,
           regioncode, region_name, geojson
    FROM   visualization.nsmis_household_sanitation_reports_vis
    ORDER  BY lgacode, lga_name, reportdate, createdat
)
SELECT  r.lgacode,
        r.lga_name,
        r.reportdate,
        r.regioncode,
        r.region_name,
        AVG(v.improved_perc_hhs)        AS avg_improved_perc_hhs,
        AVG(v.handwashstation_perc_hhs) AS avg_handwashstation_perc_hhs,
        AVG(v.handwashsoap_perc_hhs)    AS avg_handwashsoap_perc_hhs,
        r.geojson
FROM    ranked_data r
JOIN    visualization.nsmis_household_sanitation_reports_vis v
       ON v.lgacode    = r.lgacode
      AND v.lga_name   = r.lga_name
      AND v.reportdate = r.reportdate
GROUP BY r.lgacode, r.lga_name, r.reportdate,
         r.regioncode, r.region_name, r.geojson;

END;
$$;


