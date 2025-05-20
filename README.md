# About the Data Monitoring and Visualization (DMV) repository 

## Introduction 

This repository contains the processing scripts for the data on the [Unified Wash Performance Dashboard - Tanzania](https://rsdms.ruwasa.go.tz:8066/), being managed and maintained by the Rural Water and Sanitation Agency (RUWASA), Ministry of Water, Tanzania. The Unified WASH Performance Dashboard, is focused on harmonizing and integrating nationwide government data from PO-RALG, MoH and RUWASA in one place with regards to rural water and sanitation infrastructure and services across households, schools, and health facilities. It is intended to give an overview of the national picture, ensure detection of data quality issues, and better inter-ministerial collaboration. 

## Data Sources

The data being processed are coming from three different sources and systems, as follows: 

1. **Ministry of Water RUWASA's RSDMS** - there are two sets of data sources here:
   - a. Water Service and Infrastructure Coverage Data  
   - b. Water Point Functionality Data 

2. **Ministry of Health's (MoH) NSMIS**:
   - a. household sanitation reports
   - b. health care facility WASH (pending)

3. **President's Office of Regional and Local Government (PO-RALG)'s BEMIS**:
   - a. data on WASH in schools
  
## Scripts

The scripts in this repository process each of these data sources either.. 
- a. individually or 
- b. in combination with eachother or administrative codes or boundary data 

...in order to create either individual or cross-cutting visualizations. 

In the process of aggregation, **quality checks** (labelled QC in code) are conducted on the data to identify issues such as... 
- any issues such as values out of reasonable bounds,
- too few or too many observations,
- imperfect matches in the number of observations between datasets,
- any missing or unexpected data.

It is expected that subsequent work will be conducted to automatically detect errors in the data and send back a summary of the data report to the original provider of the data for human revisions and correction.  


Here is a description of each of the scripts, the input data sources and output data tables, which are subsequently used in visualizations on the rsdms system (https://rsdms.ruwasa.go.tz:8066/). 

1. **convert_wkt_to_gejson_feature.sql** - this script creates a function that takes shapefile data and converts them to geojson data, necessary for visualization of maps on Apache Superset  
2. **process_region_district_lga_names.sql** - this script combines the names and codes of the regions, districts and LGAs being used across RUWASA, MoH and PO-RALG so that cross cutting visualizations are possible in process 8. 
3. **process_gps_point_data.sql** - this script processes GPS data on water points, cleans it to get down to only key information for visualization (Edgar - does it also remove points outside Tanzania's boundaries?) 
4. **process_ruwasa_district_infracoverage.sql** - (this needs to be converted to a process by edgar) this script 
5. **process_ruwasa_wp_report.sql** - (this needs to be converted to a process by edgar)
6. **process_nsmis_data_function_fn.sql** - this script processes the MoH data on households sanitation - including creation of the variables for improved sanitation from the original toilet types, as well as data on hand hygeine access. 
7. **process_bemis_data.sql** - this script processes the PO-RALG data on BEMIS for visualization of variables such as improved water and sanitation in schools 
9. **process_cross_cutting_wash_data.sql** - this script combines other created tables to create cross cutting visualization tables at LGA and region level 


Finally, in the folder 'quality checks' once the scripts are run, a log file is produced and output with each script, which summarizes the output produced, and any potential errors found with out of bounds values. 

# Running the scripts
In order to run any of the procedure scripts created you can simply put "CALL [name_of_process_you_wish_to_call]()". For example if I wanted to call process number 6 because the system received new data from NSMIS I would go to sql lab, open an untitled query and write "CALL process_nsmis_data_function_fn();" then run it. 

# A note on schemas 

A schema is considered the “blueprint” of a database which describes how the data may relate to other tables or other data models. The two schema on to which the data are brought in the DMV dashboards are the public schema or the foreign schema:

1. **public** - data directly imported from PO-RALG and MoH on schools, sanitation and health facilities WASH
2. **foreign** - data directly imported from RSDMS on water points, supply and CBWSOs. It is called ‘foreign’ as it is foreign to the RSDMS.  

Once the data is processed, output tables are produced. These can go to: 
3. The **visualization** schema which is where the tables for final visualization on the dashboard are shared. 
4. A schema called **quality_checks** has also been created so that tables can be viewed of summary information on each of the datasets once the scripts are run through. 

## Access to the DMV

Please note that a username and login is required to access this internal system. If you are associated with the project, please contact Fravius Kalisa kalisafravy@gmail.com to request your credentials. 

## Acknowledgements 

The initial creation of this repository was enabled through a Technical Assistance from the Quality Infrastructure Improvement Trust Fund, World Bank to the Sustainable Rural Water Supply and Sanitation Program (SRWSSP) Program for Results (PforR) 2023-2025. Developments subsequent to May 2025 are the work of the team from RUWASA, Government of Tanzania. 

