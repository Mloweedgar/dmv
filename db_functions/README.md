# DMV Data Pipeline: Procedures and Orchestration

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
- **Purpose:** Build `visualization.region_district_lga_names` as a lookup for region, district, and LGA names.
- **Outputs:** `visualization.region_district_lga_names`
- **Dependencies:**
  - `visualization.ruwasa_lgas_with_geojson` (produced by previous step)
  - `public.ruwasa_districts` (external, must be loaded)
  - `public.ruwasa_regions` (external, must be loaded)

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
- **Purpose:** Utility to convert WKT geometry columns to GeoJSON Feature columns in any table.
- **Outputs:** Updates specified table/column.
- **Dependencies:** None fixed; depends on runtime arguments.
- **Notes:** Not part of main pipeline; run as needed for spatial prep.

## Dependency Graph and Execution Order

```
[External ETL Loads]
    |
    v
process_ruwasa_lgas_with_geojson
    |
    v
process_region_district_lga_names
    |         |                |                |
    |         |                |                |
    v         v                v                v
process_bemis_data   process_ruwasa_wp_report   process_ruwasa_district_infracoverage   process_nsmis_data
    |                   |                |                |
    v                   v                v                v
process_gps_point_data   process_cross_cutting_wash_data
```

**Execution Order (as enforced by `process_all_dmv_data`):**
1. External ETL Loads (all `public.*` and `foreign_schema_*` tables)
2. `process_ruwasa_lgas_with_geojson`
3. `process_region_district_lga_names`
4. `process_bemis_data`
5. `process_ruwasa_wp_report`
6. `process_ruwasa_district_infracoverage`
7. `process_nsmis_data`
8. `process_gps_point_data`
9. `process_cross_cutting_wash_data`

The orchestration procedure (`process_all_dmv_data`) enforces this order and checks all dependencies before each step.

## How to Run the Pipeline

1. **Prepare the Environment:**
   - Ensure you have access to the target database with appropriate permissions.
   - All required raw/external tables (in `public.*` and `foreign_schema_*`) must be loaded and up-to-date.

2. **Run the Orchestration Procedure:**
   - Connect to the database using your preferred SQL client.
   - Execute:
     ```sql
     CALL process_all_dmv_data();
     ```
   - The procedure will check all dependencies and run each step in order.

3. **Verify Completion:**
   - On success, you will see a notice: `All DMV data processing procedures completed successfully.`
   - Check the `visualization` schema for all expected output tables.

## Adding or Modifying Procedures

- **Best Practices:**
  - Document the purpose, outputs, and all dependencies at the top of each procedure file.
  - Use explicit dependency checks (see examples in `process_all_dmv_data.sql`).
  - Ensure new procedures are idempotent (safe to re-run).
- **Updating the Pipeline:**
  - Add your new procedure to the orchestration procedure in the correct order.
  - Update this README to reflect new or changed dependencies and outputs.

## Error Handling and Troubleshooting

- **Dependency Checks:**
  - Each step in the orchestration checks for required tables/views and raises a clear exception if missing.
- **Common Errors:**
  - *Missing dependency*: Ensure all required raw tables are loaded and up-to-date.
  - *Permission denied*: Check your database user permissions.
- **Logs and Messages:**
  - Errors and notices are output to the SQL client. Review the output for details.

## Automation and Scheduling

- **Recommended Automation:**
  - Schedule the orchestration procedure to run after all ETL loads are complete (e.g., via cron, Airflow, or a database scheduler).
- **Suggested Schedules:**
  - Monthly or quarterly, depending on data update frequency and reporting needs.

## Handover and Maintenance Notes

- **Key Risks:**
  - If any external source table is missing or stale, outputs will be incomplete or incorrect.
  - If a visualization table is missing its producing procedure, automation will break.
- **Manual Steps:**
  - Ensure all raw data loads are complete before running the pipeline.


## Change Management

- **Committing Changes:**
  - Use `git add`, `git commit`, and `git push` to version and share changes.
- **Testing:**
  - Test new or modified procedures in a development environment before deploying to production.
- **Code Review:**
  - Use pull requests and code reviews to ensure quality and maintainability.

---

**For further details, see the comments in `process_all_dmv_data.sql`, which serve as the single source of truth for execution order and dependencies.** 