# About the Data Monitoring and Visualization (DMV) repository 

This repository contains the processing scripts for the data on the Unified Wash Performance Dashboard - Tanzania [https://rsdms.ruwasa.go.tz:8066/], being managed and maintained by the Rural Water and Sanitation Agency (RUWASA), Ministry of Water, Tanzania. The data being processed are coming from three different sources and systems, as follows: 

1. RUWASA's RSDMS - there are two sets of data sources here:
   a. Water Service and Infrastructure Coverage Data  
   b. Water Point Functionality Data 

2. Ministry of Health's NSMIS:
   a. household sanitation reports
   b. health care facility WASH (pending)

3. President's Office of Regional and Local Government (PO-RALG)'s BEMIS:
   a. data on WASH in schools 

The scripts in this repository process each of these data sources either (a) individually or (b) in combination with eachother or administrative codes or boundary data in order to create cross-cutting visualizations. Here is a description of each of the scripts, the input data sources and output data tables, which are subsequently used in visualizations on the rsdms system (https://rsdms.ruwasa.go.tz:8066/) 

1. administrative_boundaries_master_dictionary.sql - this file is creating a master dictionary of codes and names of regions, districts and lgas across all three data sources listed above. This codebook is used 



Finally, in the folder 'quality checks' once the scripts are run, a log file is produced and output with each script, which summarizes the output produced, and any potential errors found with out of bounds values. 

# A note on schemas 

The flow of data comes in from one of two schemas in the dashboards: 
a. Public - data directly imported from PO-RALG and MoH on schools, sanitation and health facilities WASH
b. Foreign - data directly imported from RSDMS on water points, supply and CBWSOs

The data is subsequently transferred to the visualization schema which is where the tables for final visualization on the dashboard are shared. 
