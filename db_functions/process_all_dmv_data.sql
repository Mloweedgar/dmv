-- ============================================================================
-- process_all_dmv_data: Orchestrate all DMV data processing procedures
--
-- This procedure runs all data processing and visualization-building procedures
-- in the correct dependency order, ensuring all outputs are reliably produced.
--
-- CALLED PROCEDURES (in order):
--   1. process_ruwasa_lgas_with_geojson
--      - Produces: visualization.ruwasa_lgas_with_geojson (LGA shapes with GeoJSON)
--   2. process_region_district_lga_names
--      - Produces: visualization.region_district_lga_names (region/district/LGA lookup)
--   3. process_bemis_data
--      - Produces: visualization.bemis_school_comb_vis (combined BEMIS school data)
--   4. process_ruwasa_wp_report
--      - Produces: visualization.ruwasa_wp_report_vis, ru_wasa_wps_district, ru_wasa_wps_district_quarterly
--   5. process_ruwasa_district_infracoverage
--      - Produces: visualization.ruwasa_district_infracoverage (district coverage)
--   6. process_nsmis_data
--      - Produces: visualization.nsmis_household_sanitation_reports_vis, nsmis_household_sanitation_lga, dmv_data_quality_flags
--   7. process_gps_point_data
--      - Produces: visualization.water_point_report_with_locations (water point locations)
--   8. process_cross_cutting_wash_data
--      - Produces: visualization.cross_cutting_wash_data_vis, ruwasa_service_level_lga, nsmis_household_sanitation_reports_lga
--
-- DEPENDENCY-DRIVEN EXECUTION ORDER:
--   - Each procedure is called only after its dependencies are satisfied.
--   - All external tables (public.*, foreign_schema_*) must be loaded before running.
--
-- RISKS & SPECIAL CONSIDERATIONS:
--   - If any external source table is missing or stale, outputs will be incomplete or incorrect.
--   - If a visualization table is missing its producing procedure, automation will break.
--
-- RECOMMENDED SCHEDULE:
--   - Run after all external data loads are complete (e.g., monthly or quarterly, as needed).
--   - Safe to re-run (idempotent).
-- ============================================================================

CREATE OR REPLACE PROCEDURE public.process_all_dmv_data()
LANGUAGE plpgsql
AS $$
DECLARE
    -- Helper variables for dependency checks
    v_count INTEGER;
BEGIN
    --------------------------------------------------------------------------
    -- STEP 1: Check and Build RUWASA LGAs with GeoJSON
    --
    -- Dependency: public.ruwasa_lgas, public.tz_lgas (must be loaded externally)
    -- Output: visualization.ruwasa_lgas_with_geojson
    -- Why: All downstream spatial joins depend on this table for LGA shapes.
    --------------------------------------------------------------------------
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ruwasa_lgas';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.ruwasa_lgas'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'tz_lgas';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.tz_lgas'; END IF;
    CALL process_ruwasa_lgas_with_geojson();

    --------------------------------------------------------------------------
    -- STEP 2: Build Region/District/LGA Names Lookup
    --
    -- Dependency: visualization.ruwasa_lgas_with_geojson, public.ruwasa_districts, public.ruwasa_regions
    -- Output: visualization.region_district_lga_names
    -- Why: Used for all region/district/LGA name lookups in reporting.
    --------------------------------------------------------------------------
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ruwasa_districts';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.ruwasa_districts'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ruwasa_regions';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.ruwasa_regions'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'ruwasa_lgas_with_geojson';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.ruwasa_lgas_with_geojson'; END IF;
    CALL process_region_district_lga_names();

    --------------------------------------------------------------------------
    -- STEP 3: Build BEMIS Combined School Data
    --
    -- Dependency: public.bemis_school_services, public.bemis_school_reports, public.bemis_school_infrastructure, public.bemis_school_enrollment, visualization.region_district_lga_names
    -- Output: visualization.bemis_school_comb_vis
    -- Why: Central table for BEMIS school reporting and quality checks.
    --------------------------------------------------------------------------
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'bemis_school_services';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.bemis_school_services'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'bemis_school_reports';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.bemis_school_reports'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'bemis_school_infrastructure';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.bemis_school_infrastructure'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'bemis_school_enrollment';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.bemis_school_enrollment'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'region_district_lga_names';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.region_district_lga_names'; END IF;
    CALL process_bemis_data();

    --------------------------------------------------------------------------
    -- STEP 4: Build RUWASA Water Points Report Tables
    --
    -- Dependency: public.ruwasa_waterpoints_report, visualization.region_district_lga_names
    -- Output: visualization.ruwasa_wp_report_vis, ru_wasa_wps_district, ru_wasa_wps_district_quarterly
    -- Why: Provides water point data for further reporting and spatial joins.
    --------------------------------------------------------------------------
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ruwasa_waterpoints_report';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.ruwasa_waterpoints_report'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'region_district_lga_names';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.region_district_lga_names'; END IF;
    CALL process_ruwasa_wp_report();

    --------------------------------------------------------------------------
    -- STEP 5: Build RUWASA District Infracoverage Table
    --
    -- Dependency: foreign_schema_ruwasa_rsdms.ruwasa_reports_coverage, visualization.region_district_lga_names
    -- Output: visualization.ruwasa_district_infracoverage
    -- Why: Used for district-level coverage reporting.
    --------------------------------------------------------------------------
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'foreign_schema_ruwasa_rsdms' AND table_name = 'ruwasa_reports_coverage';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: foreign_schema_ruwasa_rsdms.ruwasa_reports_coverage'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'region_district_lga_names';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.region_district_lga_names'; END IF;
    CALL process_ruwasa_district_infracoverage();

    --------------------------------------------------------------------------
    -- STEP 6: Build NSMIS Household Sanitation Visualization Tables
    --
    -- Dependency: public.nsmis_household_sanitation_reports, public.ruwasa_regions, public.ruwasa_lgas, visualization.ruwasa_lgas_with_geojson
    -- Output: visualization.nsmis_household_sanitation_reports_vis, nsmis_household_sanitation_lga, dmv_data_quality_flags
    -- Why: Provides household sanitation data for cross-sector analysis.
    --------------------------------------------------------------------------
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'nsmis_household_sanitation_reports';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.nsmis_household_sanitation_reports'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ruwasa_regions';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.ruwasa_regions'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ruwasa_lgas';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.ruwasa_lgas'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'ruwasa_lgas_with_geojson';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.ruwasa_lgas_with_geojson'; END IF;
    CALL process_nsmis_data();

    --------------------------------------------------------------------------
    -- STEP 7: Build Water Point Report with Locations
    --
    -- Dependency: visualization.ruwasa_wp_report_vis, visualization.region_district_lga_names, foreign_schema_ruwasa_rsdms.ruwasa_villages
    -- Output: visualization.water_point_report_with_locations
    -- Why: Provides spatially joined water point data for mapping and reporting.
    --------------------------------------------------------------------------
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'ruwasa_wp_report_vis';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.ruwasa_wp_report_vis'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'region_district_lga_names';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.region_district_lga_names'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'foreign_schema_ruwasa_rsdms' AND table_name = 'ruwasa_villages';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: foreign_schema_ruwasa_rsdms.ruwasa_villages'; END IF;
    CALL process_gps_point_data();

    --------------------------------------------------------------------------
    -- STEP 8: Build Cross-Cutting WASH Visualization Tables
    --
    -- Dependency: foreign_schema_ruwasa_rsdms.ruwasa_villages, visualization.ruwasa_lgas_with_geojson, visualization.nsmis_household_sanitation_reports_vis, visualization.ruwasa_wps_district, public.ruwasa_districts
    -- Output: visualization.cross_cutting_wash_data_vis, ruwasa_service_level_lga, nsmis_household_sanitation_reports_lga
    -- Why: Final cross-sector aggregation for reporting and dashboards.
    --------------------------------------------------------------------------
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'foreign_schema_ruwasa_rsdms' AND table_name = 'ruwasa_villages';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: foreign_schema_ruwasa_rsdms.ruwasa_villages'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'ruwasa_lgas_with_geojson';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.ruwasa_lgas_with_geojson'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'nsmis_household_sanitation_reports_vis';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.nsmis_household_sanitation_reports_vis'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'visualization' AND table_name = 'ruwasa_wps_district';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: visualization.ruwasa_wps_district'; END IF;
    PERFORM 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'ruwasa_districts';
    IF NOT FOUND THEN RAISE EXCEPTION 'Missing dependency: public.ruwasa_districts'; END IF;
    CALL process_cross_cutting_wash_data();

    --------------------------------------------------------------------------
    -- END OF PIPELINE
    --------------------------------------------------------------------------
    RAISE NOTICE 'All DMV data processing procedures completed successfully.';
END;
$$; 