-- 0. Overview of each of the relevant RAW tables 
----------------------------------------------------------------------------------------------------------------------

SELECT *
FROM public.ruwasa_waterpoints_report 
LIMIT 10;
----------------------------------------------------------------------------------------------------------------------
-- 1. Create a copy and create the variables needed before collapse 
----------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS visualization.ruwasa_wp_report_vis; 
CREATE TABLE visualization.ruwasa_wp_report_vis AS
SELECT * FROM public.ruwasa_waterpoints_report;

-- take a look at it in case 
SELECT *
FROM visualization.ruwasa_wp_report_vis
LIMIT 10; 
-- doesn't have any region, lga or district names just codes. Need to be matched 
----------------------------------------------------------------------------------------------------------------------
--a. Just check the functionality breakdown in the raw data nationally 
SELECT 
    functionalitystatus, 
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM visualization.ruwasa_wp_report_vis
GROUP BY functionalitystatus
ORDER BY count DESC;

-- add dummy columns for average calculations
ALTER TABLE visualization.ruwasa_wp_report_vis  
  ADD COLUMN wp_functional INTEGER,  
  ADD COLUMN wp_functional_needs_repair INTEGER,  
  ADD COLUMN wp_non_functional INTEGER,  
  ADD COLUMN wp_abandoned_archived INTEGER,  
  ADD COLUMN wp_inconstruction INTEGER;
  
UPDATE visualization.ruwasa_wp_report_vis  
SET  
    wp_functional = CASE WHEN functionalitystatus = 'functional' THEN 1 ELSE 0 END,  
    wp_functional_needs_repair = CASE WHEN functionalitystatus = 'functional_needs_repair' THEN 1 ELSE 0 END,  
    wp_non_functional = CASE WHEN functionalitystatus = 'non_functional' THEN 1 ELSE 0 END,  
    wp_abandoned_archived = CASE WHEN functionalitystatus IN ('abandoned', 'archived') THEN 1 ELSE 0 END,  
    wp_inconstruction = CASE WHEN functionalitystatus = 'on_construction' THEN 1 ELSE 0 END;

----------------------------------------------------------------------------------------------------------------------
-- add another column for a functionality metric 
ALTER TABLE visualization.ruwasa_wp_report_vis  
  ADD COLUMN wp_status_numeric INTEGER;

UPDATE visualization.ruwasa_wp_report_vis 
SET wp_status_numeric = 
    CASE 
        WHEN functionalitystatus = 'functional' THEN 1
        WHEN functionalitystatus = 'functional_needs_repair' THEN .5
        WHEN functionalitystatus IN ('non_functional', 'abandoned') THEN 0
        WHEN functionalitystatus IN ('archived', 'on_construction') THEN NULL
    END;
----------------------------------------------------------------------------------------------------------------------
-- recast the report_year and the quarter as numeric 
ALTER TABLE visualization.ruwasa_wp_report_vis  
ALTER COLUMN report_year TYPE INTEGER USING report_year::INTEGER;

ALTER TABLE visualization.ruwasa_wp_report_vis  
ADD COLUMN report_date DATE;

UPDATE visualization.ruwasa_wp_report_vis  
SET report_date = TO_DATE(report_year::TEXT, 'YYYY');

SELECT DISTINCT quarter
FROM visualization.ruwasa_wp_report_vis ; 

ALTER TABLE visualization.ruwasa_wp_report_vis  
ALTER COLUMN quarter TYPE INTEGER USING 
    CASE 
        WHEN quarter = 'Q1' THEN 1
        WHEN quarter = 'Q2' THEN 2
        WHEN quarter = 'Q3' THEN 3
        WHEN quarter = 'Q4' THEN 4
    END;
    
----------------------------------------------------------------------------------------------------------------------
--b. calculate the averages at DISTRICT level and collapse at that level, by year 
----------------------------------------------------------------------------------------------------------------------

-- first just check how many observations by report report_year
SELECT 
    report_year, 
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM visualization.ruwasa_wp_report_vis
GROUP BY report_year
ORDER BY count DESC;

-- so we have data from 2021-2024 but only 6% of obs are from 2021, rest are evenly distributed between other years 
----------------------------------------------------------------------------------------------------------------------
-- create table by DISTRICT by year (remember to add the code after)
----------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS visualization.ruwasa_wps_district;
CREATE TABLE visualization.ruwasa_wps_district AS  
SELECT  
    district,  
    report_year,  
    AVG(wp_abandoned_archived) AS avg_wp_abandoned,  
    AVG(wp_functional) AS avg_wp_functional,  
    AVG(wp_functional_needs_repair) AS avg_wp_functional_needs_repair,  
    AVG(wp_non_functional) AS avg_wp_non_functional,
    AVG(wp_inconstruction) AS avg_in_construction,
    AVG(wp_status_numeric) AS functionality_rate
FROM visualization.ruwasa_wp_report_vis  
GROUP BY district, report_year;

-- Now just neeed to join the district names so that we can display information_schema
ALTER TABLE visualization.ruwasa_wps_district 
  ADD COLUMN regioncode VARCHAR, 
  ADD COLUMN districtname VARCHAR(255);
  UPDATE visualization.ruwasa_wps_district AS wpd
  SET 
    regioncode = d.regioncode, 
    districtname = d.districtname
  FROM public.ruwasa_districts AS d
  WHERE wpd.district = d.districtcode;
  
-- check how many districts joined
SELECT 
    COUNT(DISTINCT district) AS distinct_district_count  
    FROM visualization.ruwasa_wp_report_vis ;
-- 137 districts 

----------------------------------------------------------------------------------------------------------------------
-- create table by DISTRICT by quarter
----------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS visualization.ruwasa_wps_district_quarterly;
CREATE TABLE visualization.ruwasa_wps_district_quarterly AS  
SELECT  
    district,  
    report_year,  
    AVG(wp_abandoned_archived) AS avg_wp_abandoned,  
    AVG(wp_functional) AS avg_wp_functional,  
    AVG(wp_functional_needs_repair) AS avg_wp_functional_needs_repair,  
    AVG(wp_non_functional) AS avg_wp_non_functional,
    AVG(wp_inconstruction) AS avg_in_construction,
    AVG(wp_status_numeric) AS functionality_rate
FROM visualization.ruwasa_wp_report_vis  
GROUP BY district, report_year, quarter;

-- Now just neeed to join the district names so that we can display information_schema
ALTER TABLE visualization.ruwasa_wps_district_quarterly 
  ADD COLUMN regioncode VARCHAR, 
  ADD COLUMN districtname VARCHAR(255);
  UPDATE visualization.ruwasa_wps_district_quarterly AS wpd
  SET 
    regioncode = d.regioncode, 
    districtname = d.districtname
  FROM public.ruwasa_districts AS d
  WHERE wpd.district = d.districtcode;
  
-- note that I'm not paying attention to data cleaning within the public dataset
-- edgar to debug so runs smoothly
-- map of water point gps coordinates (for Friday or Saturday)
-- functionality at district level and region 
-- cross graph of service level at LGA level (aggregated from ruwasa_villages  vs. functionality) 