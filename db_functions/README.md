# DMV Data Pipeline: Procedures and Orchestration

> **Note:** This document focuses on running and managing the DMV database functions and procedures. For a general overview, see the [main README](../README.md). For data preparation and ETL details, see [DMV - Data Preparation Document](../DMV%20-%20Data%20Preparation%20Document.md).

## Project Overview

The DMV data pipeline is a robust, automated system for processing, aggregating, and preparing data for reporting and visualization. It is designed to support data-driven decision-making by transforming raw data from multiple sources into clean, analysis-ready tables in the `visualization` schema. The pipeline is implemented as a series of SQL procedures, orchestrated to ensure all dependencies are satisfied and outputs are reliably produced.

**Key Features:**
- Modular SQL procedures for each major data transformation step
- Explicit dependency checks for robust automation
- Central orchestration procedure (`process_all_dmv_data`) for end-to-end execution
- Designed for maintainability, extensibility, and easy handover

## Folder and File Structure

- **db_functions/**: Contains all SQL procedures for data processing and orchestration.
  - Each file defines a single procedure, with clear documentation and dependency notes.
  - `process_all_dmv_data.sql`: The master orchestration procedure that runs all others in the correct order.

## Procedure Documentation

Below is a summary of each procedure in `db_functions`, including its purpose, outputs, dependencies, and special notes.

### 1. `process_ruwasa_lgas_with_geojson`
- **Purpose:** Build `visualization.ruwasa_lgas_with_geojson` by joining LGA data with GeoJSON .
- **Outputs:** `visualization.ruwasa_lgas_with_geojson`
- **Dependencies:**
  - `public.ruwasa_lgas` (external, must be loaded)
  - `public.tz_lgas` (external, must be loaded)
- **Notes:** Run first; all spatial joins depend on this table.

### 2. `process_region_district_lga_names`
- **Purpose:** Build `visualization.region_district_lga_names` as a canonical lookup for region, district, and LGA names and codes.
- **Outputs:** `visualization.region_district_lga_names`
- **Dependencies:**
  - `public.ruwasa_lgas` (raw, external, must be loaded)
  - `public.ruwasa_districts` (raw, external, must be loaded)
  - `public.ruwasa_regions` (raw, external, must be loaded)
- **Notes:** Run after the three source tables above are loaded and current. Downstream procedures rely on this lookup; run it before any step that references `visualization.region_district_lga_names`.

### 2a. `process_region_district_lga_lookup`
- **Purpose:** Build `visualization.region_district_lga_lookup` as a comprehensive lookup for region, district, and LGA codes/names, supporting cross-cutting visualizations and harmonization across data sources.
- **Outputs:** `visualization.region_district_lga_lookup`
- **Dependencies:**
  - `visualization.ruwasa_lgas_with_geojson` (produced by previous step)
  - `public.ruwasa_districts` (external, must be loaded)
  - `public.ruwasa_regions` (external, must be loaded)
- **Notes:** This procedure supplements (but does not replace) `process_region_district_lga_names`. Use both lookup tables as required by downstream processes.

### 3. `process_bemis_data`
- **Purpose:** Build `visualization.bemis_school_comb_vis` by combining and enriching BEMIS school data.
- **Outputs:** `visualization.bemis_school_comb_vis`
- **Dependencies:**
  - `public.bemis_school_services` (external, must be loaded)
  - `public.bemis_school_reports` (external, must be loaded)
  - `public.bemis_school_infrastructure` (external, must be loaded)
  - `public.bemis_school_enrollment` (external, must be loaded)
  - `visualization.region_district_lga_names` (produced by previous step)

### 4. `process_ruwasa_wp_report`
- **Purpose:** Aggregate and enrich RUWASA water points data for reporting.
- **Outputs:**
  - `visualization.ruwasa_wp_report_vis`
  - `visualization.ruwasa_wps_district`
  - `visualization.ruwasa_wps_district_quarterly`
- **Dependencies:**
  - `public.ruwasa_waterpoints_report` (external, must be loaded)
  - `visualization.region_district_lga_names` (produced by previous step)

### 5. `process_ruwasa_district_infracoverage`
- **Purpose:** Aggregate RUWASA coverage data at the district level for reporting.
- **Outputs:** `visualization.ruwasa_district_infracoverage`
- **Dependencies:**
  - `foreign_schema_ruwasa_rsdms.ruwasa_reports_coverage` (external, must be loaded)
  - `visualization.region_district_lga_names` (produced by previous step)

### 6. `process_nsmis_data`
- **Purpose:** Build NSMIS household sanitation visualization tables and update data quality flags.
- **Outputs:**
  - `visualization.nsmis_household_sanitation_reports_vis`
  - `visualization.nsmis_household_sanitation_lga`
  - `visualization.dmv_data_quality_flags`
- **Dependencies:**
  - `public.nsmis_household_sanitation_reports` (external, must be loaded)
  - `public.ruwasa_regions` (external, must be loaded)
  - `public.ruwasa_lgas` (external, must be loaded)
  - `visualization.ruwasa_lgas_with_geojson` (produced by first step)

### 7. `process_gps_point_data`
- **Purpose:** Build `visualization.water_point_report_with_locations` by joining water point data with location info.
- **Outputs:** `visualization.water_point_report_with_locations`
- **Dependencies:**
  - `visualization.ruwasa_wp_report_vis` (produced by previous step)
  - `visualization.region_district_lga_names` (produced by previous step)
  - `foreign_schema_ruwasa_rsdms.ruwasa_villages` (external, must be loaded)

### 8. `process_cross_cutting_wash_data`
- **Purpose:** Aggregate and join NSMIS, RUWASA, and spatial data to produce cross-sector WASH visualization tables.
- **Outputs:**
  - `visualization.cross_cutting_wash_data_vis`
  - `visualization.ruwasa_service_level_lga`
  - `visualization.nsmis_household_sanitation_reports_lga`
- **Dependencies:**
  - `foreign_schema_ruwasa_rsdms.ruwasa_villages` (external, must be loaded)
  - `visualization.ruwasa_lgas_with_geojson` (produced by first step)
  - `visualization.nsmis_household_sanitation_reports_vis` (produced by previous step)
  - `visualization.ruwasa_wps_district` (produced by previous step)
  - `public.ruwasa_districts` (external, must be loaded)

### 9. `convert_wkt_to_geojson_feature`