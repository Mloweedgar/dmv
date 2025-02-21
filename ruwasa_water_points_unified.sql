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

-- need to make report_year a date time variable otherwise it won't let me create the evolution chart 
ALTER TABLE visualization.ruwasa_wp_report_vis ADD COLUMN year_timestamp TIMESTAMP;
UPDATE visualization.ruwasa_wp_report_vis SET year_timestamp = TO_TIMESTAMP(report_year::TEXT, 'YYYY');

----------------------------------------------------------------------------------------------------------------------
-- 2. Add new variables like date time format for quarter, region, lga, district codes and functionality dummies 
----------------------------------------------------------------------------------------------------------------------
-- a. Date time of monthly and quarterly variables 

-- make a monthly variable from the date time variable that is granular from report_datetime
ALTER TABLE visualization.ruwasa_wp_report_vis ADD COLUMN month_timestamp TIMESTAMP;
UPDATE visualization.ruwasa_wp_report_vis SET month_timestamp = DATE_TRUNC('month', report_datetime);

SELECT DISTINCT month_timestamp from visualization.ruwasa_wp_report_vis; 

-- make a quarterly variable from the date time variable that is granular from report_datetime
ALTER TABLE visualization.ruwasa_wp_report_vis ADD COLUMN quarter_timestamp TIMESTAMP;
UPDATE visualization.ruwasa_wp_report_vis SET quarter_timestamp = DATE_TRUNC('quarter', report_datetime);

-- check 
SELECT DISTINCT month_timestamp from visualization.ruwasa_wp_report_vis; 
SELECT DISTINCT quarter_timestamp from visualization.ruwasa_wp_report_vis ;

----------------------------------------------------------------------------------------------------------------------
--b. Add region, district names 

ALTER TABLE visualization.ruwasa_wp_report_vis 
  ADD COLUMN region_code VARCHAR,
  ADD COLUMN region_name VARCHAR,
  ADD COLUMN district_name VARCHAR(255);
  
  
UPDATE visualization.ruwasa_wp_report_vis AS wpd
  SET 
    region_code = d.region_code,
    region_name = d.region_name,
    district_name = d.district_name
  FROM visualization.region_district_lga_names  AS d
  WHERE wpd.district = d.district_code ;
  
ALTER TABLE visualization.ruwasa_wp_report_vis
RENAME COLUMN district TO district_code;
  
----------------------------------------------------------------------------------------------------------------------
--c. Add functionality dummies 

-- Just check the functionality breakdown in the raw data nationally 
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
  ADD COLUMN wp_inconstruction INTEGER,
  ADD COLUMN wp_func_denominator INTEGER;
  
UPDATE visualization.ruwasa_wp_report_vis  
SET  
    wp_functional = CASE WHEN functionalitystatus = 'functional' THEN 1 ELSE 0 END,  
    wp_functional_needs_repair = CASE WHEN functionalitystatus = 'functional_need_repair' THEN 1 ELSE 0 END,  
    wp_non_functional = CASE WHEN functionalitystatus = 'not_functional' THEN 1 ELSE 0 END,  
    wp_abandoned_archived = CASE WHEN functionalitystatus IN ('abandoned', 'archived') THEN 1 ELSE 0 END,  
    wp_inconstruction = CASE WHEN functionalitystatus = 'on_construction' THEN 1 ELSE 0 END,
    wp_func_denominator = CASE WHEN functionalitystatus IN('functional', 'functional_needs_repair', 'not_functional')THEN 1 ELSE 0 END;

----------------------------------------------------------------------------------------------------------------------
-- add another column for a functionality metric (optional to be considered)
ALTER TABLE visualization.ruwasa_wp_report_vis  
  ADD COLUMN wp_status_numeric INTEGER;

UPDATE visualization.ruwasa_wp_report_vis 
SET wp_status_numeric = 
    CASE 
        WHEN functionalitystatus = 'functional' THEN 1
        WHEN functionalitystatus = 'functional_needs_repair' THEN .5
        WHEN functionalitystatus IN ('not_functional', 'abandoned') THEN 0
        WHEN functionalitystatus IN ('archived', 'on_construction') THEN NULL
    END;
    
----------------------------------------------------------------------------------------------------------------------
--3. calculate the averages at DISTRICT level and collapse at that level, by year 
----------------------------------------------------------------------------------------------------------------------

-- a. first just check how many observations by report report_year
SELECT 
    report_year, 
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM visualization.ruwasa_wp_report_vis
GROUP BY report_year
ORDER BY count DESC;
-- so we have data from 2021-2024 but only 6% of obs are from 2021, rest are evenly distributed between other years 

----------------------------------------------------------------------------------------------------------------------

-- b. create table by DISTRICT by year 

DROP TABLE IF EXISTS visualization.ruwasa_wps_district;
CREATE TABLE visualization.ruwasa_wps_district AS  
SELECT  
    district_code,  
    year_timestamp, 
    district_name, 
    AVG(wp_abandoned_archived) AS avg_wp_abandoned,  
    SUM(wp_functional) AS sum_wp_functional,  
    AVG(wp_functional_needs_repair) AS avg_wp_functional_needs_repair,  
    AVG(wp_non_functional) AS avg_wp_non_functional,
    AVG(wp_inconstruction) AS avg_in_construction,
    AVG(wp_status_numeric) AS functionality_rate, 
    SUM(wp_func_denominator) AS func_denom,
    FIRST_VALUE(wp.region_code) OVER (PARTITION BY wp.district_code) AS region_code,  
    FIRST_VALUE(wp.region_name) OVER (PARTITION BY wp.district_code) AS region_name  
FROM visualization.ruwasa_wp_report_vis wp
GROUP BY district_code, district_name, year_timestamp, region_code, region_name ;


-- add new colum with functional/func_denom 
ALTER TABLE visualization.ruwasa_wps_district
  ADD COLUMN func_rate_new NUMERIC;

UPDATE visualization.ruwasa_wps_district
  SET func_rate_new = 
    CASE 
        WHEN func_denom = 0 THEN NULL  -- Avoid division by zero
        ELSE sum_wp_functional::NUMERIC / func_denom *100
    END;
  
-- check how many districts joined
SELECT 
    COUNT(DISTINCT district_code) AS distinct_district_count  
    FROM visualization.ruwasa_wps_district ;
-- 137 districts 
  
-- check header of dataset 
SELECT * from visualization.ruwasa_wps_district LIMIT 10; 


----------------------------------------------------------------------------------------------------------------------
-- 3. create table by DISTRICT by quarter
----------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS visualization.ruwasa_wps_district_quarterly ;
CREATE TABLE visualization.ruwasa_wps_district_quarterly  AS  
SELECT  
    district_code,  
    year_timestamp, 
    quarter_timestamp,
    district_name, 
    AVG(wp_abandoned_archived) AS avg_wp_abandoned,  
    SUM(wp_functional) AS sum_wp_functional,  
    AVG(wp_functional_needs_repair) AS avg_wp_functional_needs_repair,  
    AVG(wp_non_functional) AS avg_wp_non_functional,
    AVG(wp_inconstruction) AS avg_in_construction,
    AVG(wp_status_numeric) AS functionality_rate, 
    SUM(wp_func_denominator) AS func_denom,
    FIRST_VALUE(wp.region_code) OVER (PARTITION BY wp.district_code) AS region_code,  
    FIRST_VALUE(wp.region_name) OVER (PARTITION BY wp.district_code) AS region_name  
FROM visualization.ruwasa_wp_report_vis wp
GROUP BY district_code, district_name, year_timestamp, quarter_timestamp, region_code, region_name ;


-- add new colum with functional/func_denom 
ALTER TABLE visualization.ruwasa_wps_district_quarterly
  ADD COLUMN func_rate_new NUMERIC;

UPDATE visualization.ruwasa_wps_district_quarterly
  SET func_rate_new = 
    CASE 
        WHEN func_denom = 0 THEN NULL  -- Avoid division by zero
        ELSE sum_wp_functional::NUMERIC / func_denom 
    END;

-- check outputs 

SELECT *
FROM visualization.ruwasa_wps_district_quarterly 
LIMIT 10; 
  
-- note that I'm not paying attention to data cleaning within the public dataset
-- edgar to debug so runs smoothly
