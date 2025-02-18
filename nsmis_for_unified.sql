------------------------------------------ PROCESSING NSMIS DATA ----------------------------------------
--1. Check the public data that is there from NSMIS
----------------------------------------------------------------------------------------------------------------

SELECT *
  FROM public.nsmis_household_sanitation_reports LIMIT 100 ; 

SELECT *
  FROM public.nsmis_health_facilities LIMIT 100 ; -- no data there yet

SELECT * 
  FROM public.nsmis_report_statistics LIMIT 100 ; -- data is there  

--check what years data they have 
SELECT DISTINCT reportdate 
  FROM public.nsmis_household_sanitation_reports
  ORDER BY reportdate LIMIT 100 ;  -- we have from 2022 - 2024

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
UPDATE visualization.nsmis_household_sanitation_reports_vis 
SET totalhouseholds = 
    COALESCE(toilettypea, 0) + 
    COALESCE(toilettypeb, 0) + 
    COALESCE(toilettypec, 0) + 
    COALESCE(toilettyped, 0) + 
    COALESCE(toilettypee, 0) + 
    COALESCE(toilettypef, 0) + 
    COALESCE(toilettypex, 0);
    
----b. Number of improved facilities (types b-e)

ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
ADD COLUMN improved_count_hhs INTEGER;
UPDATE visualization.nsmis_household_sanitation_reports_vis 
SET improved_count_hhs = 
    COALESCE(toilettypeb, 0) + 
    COALESCE(toilettypec, 0) + 
    COALESCE(toilettyped, 0) + 
    COALESCE(toilettypee, 0);
    
-----c. % of improved facilities (types a-e)

ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
ADD COLUMN improved_perc_hhs NUMERIC;

UPDATE visualization.nsmis_household_sanitation_reports_vis 
  SET improved_perc_hhs = improved_count_hhs::DOUBLE PRECISION / NULLIF(totalhouseholds, 0);    
  
SELECT *
FROM public.ruwasa_regions
LIMIT 100 ; -- nsmisregioncode
-- join with the region and LGA keys so that they can have names on visualizations 

--- d. % with handwashingstation
ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
ADD COLUMN handwashstation_perc_hhs NUMERIC;
UPDATE visualization.nsmis_household_sanitation_reports_vis 
  SET handwashstation_perc_hhs = handwashingstation::DOUBLE PRECISION / NULLIF(totalhouseholds, 0);    


--- e. % with hwfsoapwater
ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
ADD COLUMN handwashsoap_perc_hhs NUMERIC;
UPDATE visualization.nsmis_household_sanitation_reports_vis 
  SET handwashsoap_perc_hhs = hwfsoapwater::DOUBLE PRECISION / NULLIF(totalhouseholds, 0);    


---- d. Join in region names to nsmis_households_vis


ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
  ADD COLUMN region_name VARCHAR(100);
  
UPDATE visualization.nsmis_household_sanitation_reports_vis AS target
SET region_name = source.name
FROM public.ruwasa_regions AS source
WHERE target.regioncode = source.nsmisregioncode;


---- e. Join in LGA names to nsmis_households_vis

ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
  ADD COLUMN lga_name VARCHAR(100);

UPDATE visualization.nsmis_household_sanitation_reports_vis AS target
SET lga_name = source.lganame
FROM public.ruwasa_lgas AS source
WHERE target.lgacode = source.nsmislgacode;

-- join the json shape file features 

ALTER TABLE visualization.nsmis_household_sanitation_reports_vis 
  ADD COLUMN geojson VARCHAR;
  
UPDATE visualization.nsmis_household_sanitation_reports_vis AS target
SET geojson = source.geojson
FROM public.tz_lgas AS source
WHERE target."lga_name" = source."Authority";

--  f. reorder the columns and save table to visualization schema
DROP TABLE IF EXISTS visualization.nsmis_household_sanitation_reports_v;

CREATE TABLE visualization.nsmis_household_sanitation_reports_v AS
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

drop TABLE  visualization.nsmis_household_sanitation_reports_vis;

ALTER TABLE visualization.nsmis_household_sanitation_reports_v RENAME TO nsmis_household_sanitation_reports_vis;

-- look at resulting table 
--SELECT *
  --FROM visualization.nsmis_household_sanitation_reports_v LIMIT 1000; 

SELECT *
  FROM visualization.nsmis_household_sanitation_reports_vis LIMIT 1000; 

-- check how many rows in the table 
SELECT COUNT(*) FROM visualization.nsmis_household_sanitation_reports_vis;

---------------------------------------------------------------------------------------------------------------------
-- make a summary table by LGA 
DROP TABLE IF EXISTS visualization.nsmis_household_sanitation_reports_lga;

CREATE TABLE visualization.nsmis_household_sanitation_reports_lga AS
WITH ranked_data AS (
    SELECT DISTINCT ON (lgacode, lga_name, reportdate) 
        lgacode,
        lga_name,
        reportdate,
        regioncode,
        region_name,
        geojson
    FROM visualization.nsmis_household_sanitation_reports_vis
    ORDER BY lgacode, lga_name, reportdate, createdat
)

SELECT 
    r.lgacode,
    r.lga_name,
    r.reportdate,
    r.regioncode,
    r.region_name,
    r.geojson,
    
    -- Averages of required percentage variables
    AVG(v.improved_perc_hhs) AS avg_improved_perc_hhs,
    AVG(v.handwashstation_perc_hhs) AS avg_handwashstation_perc_hhs,
    AVG(v.handwashsoap_perc_hhs) AS avg_handwashsoap_perc_hhs

FROM ranked_data r
JOIN visualization.nsmis_household_sanitation_reports_vis v
ON r.lgacode = v.lgacode 
AND r.lga_name = v.lga_name 
AND r.reportdate = v.reportdate 

GROUP BY r.lgacode, r.lga_name, r.reportdate, r.regioncode, r.region_name, r.geojson;

DROP TABLE IF EXISTS visualization.nsmis_household_sanitation_lga;

CREATE TABLE visualization.nsmis_household_sanitation_lga AS
SELECT
  regioncode,
  region_name,
  lgacode,
  lga_name,
  reportdate,
  avg_improved_perc_hhs,
  avg_handwashsoap_perc_hhs,
  avg_handwashstation_perc_hhs

  FROM visualization.nsmis_household_sanitation_reports_lga;
  
DROP TABLE visualization.nsmis_household_sanitation_reports_lga;
SELECT * from visualization.nsmis_household_sanitation_lga