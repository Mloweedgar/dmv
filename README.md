# Data Monitoring and Visualization (DMV) Repository

Welcome to the DMV repository, which contains the scripts and documentation for the [Unified Wash Performance Dashboard - Tanzania](https://rsdms.ruwasa.go.tz:8066/), managed by RUWASA, Ministry of Water, Tanzania. The dashboard harmonizes and integrates nationwide government data from PO-RALG, MoH, and RUWASA, providing a unified view of rural water and sanitation infrastructure and services across households, schools, and health facilities.

## Documentation Overview

- **[DMV - Data Preparation Document](./DMV%20-%20Data%20Preparation%20Document.md):**
  Details the data flow, ETL process, and how raw data is transformed and prepared for visualization in the DMV system.

- **[db_functions/README.md](./db_functions/README.md):**
  Documents the SQL procedures, orchestration, and how to run and manage the database functions that power the dashboard.

## Data Sources

The data processed in this repository comes from three main sources:

1. **RUWASA's RSDMS:** Water service, infrastructure coverage, and water point functionality data
2. **Ministry of Health's (MoH) NSMIS:** Household sanitation reports, health care facility WASH 
3. **PO-RALG's BEMIS:** WASH in schools

## Repository Structure

Scripts in this repository process these data sources individually or in combination, creating both individual and cross-cutting visualizations. Quality checks (QC) are included to identify data issues. For a full list and description of scripts, see [db_functions/README.md](./db_functions/README.md).

## Schemas

- **public:** Raw data from PO-RALG and MoH
- **foreign:** Raw data from RSDMS
- **visualization:** Processed/aggregated data for dashboards
- **quality_checks:** Summary tables and logs for QC

## Getting Started

- For data preparation and ETL details, see [DMV - Data Preparation Document](./DMV%20-%20Data%20Preparation%20Document.md).
- For running and managing database functions, see [db_functions/README.md](./db_functions/README.md).

## Access

A username and login are required to access the internal system. Contact Fravius Kalisa (kalisafravy@gmail.com) for credentials if you are associated with the project.

## Acknowledgements

This repository was created with support from the World Bank and RUWASA, Government of Tanzania.

