------------------------------------------ PROCESSING NSMIS DATA ----------------------------------------
--1. Check the data that is there from NSMIS
----------------------------------------------------------------------------------------------------------------

SELECT *
  FROM nsmis_household_sanitation_reports LIMIT 100 ; 

SELECT *
  FROM nsmis_health_facilities LIMIT 100 ; -- no data there yet

SELECT * 
  FROM nsmis_report_statistics LIMIT 100 ; -- data is there  

--check what years data they have 
SELECT DISTINCT reportdate 
  FROM nsmis_household_sanitation_reports_vis  -- we have from 2022 - 2024

----------------------------------------------------------------------------------------------------------------
-- 2. copy the raw input table and create a preprocessed version suitable for visualization
----------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS public.nsmis_household_sanitation_reports_vis; 
DROP TABLE IF EXISTS visualization.nsmis_household_sanitation_reports_vis; 


CREATE TABLE visualization.nsmis_household_sanitation_reports_vis AS
SELECT * FROM public.nsmis_household_sanitation_reports;

ALTER TABLE visualization.nsmis_household_sanitation_reports_vis DROP COLUMN IF EXISTS totalhouseholds;
ALTER TABLE visualization.nsmis_household_sanitation_reports_vis DROP COLUMN IF EXISTS improved_count_hhs;
ALTER TABLE visualization.nsmis_household_sanitation_reports_vis DROP COLUMN IF EXISTS improved_perc_hhs;

----------------------------------------------------------------------------------------------------------------
-- 3. Make new variables for visualization 
----------------------------------------------------------------------------------------------------------------

----a. Total households variable  (need to generate this a different way)

ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
ADD COLUMN totalhouseholds INTEGER;
UPDATE nsmis_household_sanitation_reports_vis 
SET totalhouseholds = 
    COALESCE(toilettypea, 0) + 
    COALESCE(toilettypeb, 0) + 
    COALESCE(toilettypec, 0) + 
    COALESCE(toilettyped, 0) + 
    COALESCE(toilettypee, 0) + 
    COALESCE(toilettypef, 0) + 
    COALESCE(toilettypex, 0);
    
----b. Number of improved facilities (types a-e)

ALTER TABLE nsmis_household_sanitation_reports_vis 
ADD COLUMN improved_count_hhs INTEGER;
UPDATE nsmis_household_sanitation_reports_vis 
SET improved_count_hhs = 
    COALESCE(toilettypea, 0) + 
    COALESCE(toilettypeb, 0) + 
    COALESCE(toilettypec, 0) + 
    COALESCE(toilettyped, 0) + 
    COALESCE(toilettypee, 0);
    
-----c. % of improved facilities (types a-e)

ALTER TABLE nsmis_household_sanitation_reports_vis 
ADD COLUMN improved_perc_hhs NUMERIC;
UPDATE nsmis_household_sanitation_reports_vis 
  SET improved_perc_hhs = improved_count_hhs::DOUBLE PRECISION / NULLIF(totalhouseholds, 0);    
  --ALTER COLUMN improved_perc_hhs TYPE NUMERIC(10,2) 
  --USING ROUND(improved_perc_hhs::NUMERIC, 2);

SELECT *
FROM public.ruwasa_regions
LIMIT 100 -- nsmisregioncode
-- join with the region and LGA keys so that they can have names on visualizations 

--- d. % with handwashingstation
ALTER TABLE nsmis_household_sanitation_reports_vis 
ADD COLUMN handwashstation_perc_hhs NUMERIC;
UPDATE nsmis_household_sanitation_reports_vis 
  SET handwashstation_perc_hhs = handwashstation_perc_hhs::DOUBLE PRECISION / NULLIF(totalhouseholds, 0);    


--- e. % with hwfsoapwater
ALTER TABLE nsmis_household_sanitation_reports_vis 
ADD COLUMN handwashsoap_perc_hhs NUMERIC;
UPDATE nsmis_household_sanitation_reports_vis 
  SET handwashsoap_perc_hhs = hwfsoapwater::DOUBLE PRECISION / NULLIF(totalhouseholds, 0);    


---- d. Join in region names to nsmis_households_vis


ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
  ADD COLUMN region_name VARCHAR(100);
  
UPDATE visualization.nsmis_household_sanitation_reports_vis AS target
SET region_name = source.name
FROM ruwasa_regions AS source
WHERE target.regioncode = source.nsmisregioncode;


---- e. Join in LGA names to nsmis_households_vis

ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
  ADD COLUMN lga_name VARCHAR(100);

UPDATE visualization.nsmis_household_sanitation_reports_vis AS target
SET lga_name = source.lganame
FROM ruwasa_lgas AS source
WHERE target.lgacode = source.nsmislgacode;

-- join the json shape file features 

ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
  ADD COLUMN geojson VARCHAR;
  
UPDATE visualization.nsmis_household_sanitation_reports_vis AS target
SET geojson = source.geojson
FROM tz_lgas AS source
WHERE target."lga_name" = source."Authority";

--  f. reorder the columns and save table to visualization schema
CREATE SCHEMA visualization
CREATE TABLE visualization.nsmis_household_sanitation_reports_vis AS
SELECT
    sysid,
    reportdate,
    regioncode,
    region_name,
    lgacode,
    lga_name,
    wardcode,
    wardname,
    villagecode,
    villagename,
    toilettypea,
    toilettypeb,
    toilettypec,
    toilettyped,
    toilettypee,
    toilettypef,
    toilettypex,
    handwashingstation,
    hwfsoapwater,
    createdat,
    totalhouseholds,
    improved_count_hhs,
    improved_perc_hhs,
    handwashstation_perc_hhs,
    handwashsoap_perc_hhs,
    geojson
    
FROM visualization.nsmis_household_sanitation_reports_vis;

drop visualization.nsmis_household_sanitation_reports_vis;

--- g. Make a collapsed table to just check that the figures are the same on coverage



-- look at resulting table 

SELECT *
  FROM visualization.nsmis_household_sanitation_reports_vis LIMIT 1000; 

-- check how many rows in the table 
SELECT COUNT(*) FROM nsmis_household_sanitation_reports_vis;


  