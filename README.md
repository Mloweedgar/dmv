# About the Data Monitoring and Visualization (DMV) repository 

This repository contains the processing scripts for the data on the [Unified Wash Performance Dashboard - Tanzania] (https://rsdms.ruwasa.go.tz:8066/), being managed and maintained by the Rural Water and Sanitation Agency (RUWASA), Ministry of Water, Tanzania. The data being processed are coming from three different sources and systems, as follows: 

1. **Ministry of Water RUWASA's RSDMS** - there are two sets of data sources here:
   - a. Water Service and Infrastructure Coverage Data  
   - b. Water Point Functionality Data 

2. **Ministry of Health's (MoH) NSMIS**:
   - a. household sanitation reports
   - b. health care facility WASH (pending)

3. **President's Office of Regional and Local Government (PO-RALG)'s BEMIS**:
   - a. data on WASH in schools 

The scripts in this repository process each of these data sources either (a) individually or (b) in combination with eachother or administrative codes or boundary data in order to create cross-cutting visualizations. Here is a description of each of the scripts, the input data sources and output data tables, which are subsequently used in visualizations on the rsdms system (https://rsdms.ruwasa.go.tz:8066/). Please note that a username and login is required to access this internal system. If you are associated with the project, please contact Fravius Kalisa kalisafravy@gmail.com to request your credentials. 

1. **convert_wkt_to_gejson_feature.sql** - this script creates a function that takes shapefile data and converts them to geojson data, necessary for visualization of maps on Apache Superset  
2. **process_region_district_lga_names.sql** - this script combines the names and codes of the regions, districts and LGAs being used across RUWASA, MoH and PO-RALG so that cross cutting visualizations are possible in process 8. 
3. **process_gps_point_data.sql** - this script processes GPS data on water points, cleans it to get down to only key information for visualization (Edgar - does it also remove points outside Tanzania's boundaries?) 
4. **ruwasa_infracoverage_vis** - (this needs to be converted to a process by edgar) this script 
5. **ruwasa_waterpoint_vis** - (this needs to be converted to a process by edgar)
6. **process_bemis_data.sql** - this script processes the PO-RALG data on BEMIS for visualization of variables such as improved water and sanitation in schools 
7. **process_nsmis_data_function_fn.sql** - this script processes the MoH data on households sanitation - including creation of the variables for improved sanitation from the original toilet types, as well as data on hand hygeine access. 
8. **process_cross_cutting_wash_data.sql** - this script combines other created tables to create cross cutting visualization tables at LGA and region level 




Finally, in the folder 'quality checks' once the scripts are run, a log file is produced and output with each script, which summarizes the output produced, and any potential errors found with out of bounds values. 

# A note on schemas 

The flow of data comes in from one of two schemas in the dashboards: 
1. **Public** - data directly imported from PO-RALG and MoH on schools, sanitation and health facilities WASH
2. **Foreign** - data directly imported from RSDMS on water points, supply and CBWSOs

The data is subsequently transferred to the **visualization** schema which is where the tables for final visualization on the dashboard are shared. 
