-- 0. Overview of each of the relevant RAW tables - basic checks to be converted to quality control reports
----------------------------------------------------------------------------------------------------------------------


select * FROM public.bemis_school_services LIMIT 10;
select * FROM public.bemis_school_infrastructure LIMIT 10; 
select * FROM public.bemis_school_enrollment LIMIT 10; 
select * FROM public.bemis_school_reports LIMIT 10;
select * FROM public.bemis_report_statistics LIMIT 10;


-- -- first drop the test schools 
-- select * FROM visualization.bemis_school_comb_vis where region_code='sJ3VX9rfhlb'


select count(*) FROM public.bemis_school_services ; 
-- 26,582 schools
select count(*) FROM public.bemis_school_infrastructure ;
-- 27,030 schools 
select count(*) FROM public.bemis_school_reports ; 
-- 27,370 schools 
select count(*) FROM public.bemis_report_statistics  ; 
-- 184 schools 
select count(*) FROM public.bemis_school_enrollment  ; 
-- 6487 schools 

 

---


SELECT * 
FROM public.bemis_school_infrastructure AS bsi
WHERE NOT EXISTS (
    SELECT 1 
    FROM public.bemis_school_services AS bss
    WHERE bsi.schoolregnumber = bss.schoolregnumber
);

-- 450 schools are in infrastructure, but not in services 

SELECT *
FROM visualization.region_district_lga_names
LIMIT 10;
----------------------------------------------------------------------------------------------------------------------
--1. Copy a version of each into the visualization schema for editing (create a combined table but start with services)
----------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS visualization.bemis_school_comb_vis; 
CREATE TABLE visualization.bemis_school_comb_vis AS 
SELECT * from public.bemis_school_services;
----------------------------------------------------------------------------------------------------------------------
--2. Match the table with the identifiers for region, district, lga 
----------------------------------------------------------------------------------------------------------------------
-- first add the region, lga and ward codes from bemis_school_reports

ALTER TABLE visualization.bemis_school_comb_vis  
  ADD COLUMN region_code VARCHAR,
  ADD COLUMN lga_code VARCHAR(255),
  ADD COLUMN gps_coordinates VARCHAR,
  ADD COLUMN schoolpforrprogram BOOLEAN;

  
UPDATE visualization.bemis_school_comb_vis  AS sch
  SET 
    region_code = r.regioncode,
    lga_code = r.lgacode,
    gps_coordinates = r.gpscoordinates,
    schoolpforrprogram = r.schoolpforrprogram
  FROM public.bemis_school_reports  AS r
  WHERE sch.schoolregnumber = r.schoolregnumber ;
  
SELECT * from visualization.bemis_school_comb_vis LIMIT 100; 
----------------------------------------------------------------------------------------------------------------------
-- now add the region and lga names from edgar's names table 

ALTER TABLE visualization.bemis_school_comb_vis  
  ADD COLUMN region_name VARCHAR,
  ADD COLUMN lga_name VARCHAR(255);
  
UPDATE visualization.bemis_school_comb_vis  AS sch
  SET 
    region_name = n.region_name,
    lga_name = n.lga_name
  FROM visualization.region_district_lga_names  AS n
  WHERE sch.lga_code = n.bemislgacode ;
  
-- identify the columns to be added 

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'bemis_school_infrastructure' 
AND column_name NOT IN (
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'bemis_school_comb_vis'
);
----------------------------------------------------------------------------------------------------------------------
-- add the columns from infrastructure to comb 
SELECT 
    'ALTER TABLE bemis_school_comb_vis ADD COLUMN ' || column_name || ' ' || data_type || ';' 
FROM information_schema.columns
WHERE table_name = 'bemis_school_infrastructure' 
AND column_name NOT IN (
    SELECT column_name FROM information_schema.columns 
    WHERE table_name = 'bemis_school_comb_vis'
);

ALTER TABLE visualization.bemis_school_comb_vis 
ADD COLUMN classroomcount INT,
ADD COLUMN classroomsrequired INT,
ADD COLUMN dispensaries INT,
ADD COLUMN dropholesboys INT,
ADD COLUMN dropholesgirls INT,
ADD COLUMN dropholesspecialneedsboys INT,
ADD COLUMN dropholesspecialneedsgirls INT,
ADD COLUMN dropholesmaleteachers INT,
ADD COLUMN dropholesfemaleteachers INT,
ADD COLUMN latrineslockable BOOLEAN,
ADD COLUMN latrinesmaintained BOOLEAN,
ADD COLUMN latrineshavewashablefloor BOOLEAN,
ADD COLUMN latrineswithoutwastewaterdischarge BOOLEAN,
ADD COLUMN latrinesconnectedstspcesspit BOOLEAN,
ADD COLUMN responsibleforcleanlinesstoiletsboys VARCHAR,
ADD COLUMN responsibleforcleanlinesstoiletsgirls VARCHAR,
ADD COLUMN toilettypeftconnmainsewerline BOOLEAN,
ADD COLUMN toilettypeftconnseptictank BOOLEAN,
ADD COLUMN toilettypeftconnpit BOOLEAN,
ADD COLUMN toilettypeftconnanotherloc BOOLEAN,
ADD COLUMN toilettypevippitlatrine BOOLEAN,
ADD COLUMN toilettypepitlatrinewithfloor BOOLEAN,
ADD COLUMN toilettypeopenpitlatrine BOOLEAN,
ADD COLUMN toilettypeothers BOOLEAN,
ADD COLUMN burningchambers INT,
ADD COLUMN septictanks INT,
ADD COLUMN watertanks INT,
ADD COLUMN specialgirlsroom BOOLEAN;

select count(*) from visualization.bemis_school_comb_vis


UPDATE visualization.bemis_school_comb_vis AS bscv
SET 
    classroomcount = bsi.classroomcount,
    classroomsrequired = bsi.classroomsrequired,
    dispensaries = bsi.dispensaries,
    dropholesboys = bsi.dropholesboys,
    dropholesgirls = bsi.dropholesgirls,
    dropholesspecialneedsboys = bsi.dropholesspecialneedsboys,
    dropholesspecialneedsgirls = bsi.dropholesspecialneedsgirls,
    dropholesmaleteachers = bsi.dropholesmaleteachers,
    dropholesfemaleteachers = bsi.dropholesfemaleteachers,
    latrineslockable = bsi.latrineslockable,
    latrinesmaintained = bsi.latrinesmaintained,
    latrineshavewashablefloor = bsi.latrineshavewashablefloor,
    latrineswithoutwastewaterdischarge = bsi.latrineswithoutwastewaterdischarge,
    latrinesconnectedstspcesspit = bsi.latrinesconnectedstspcesspit,
    responsibleforcleanlinesstoiletsboys = bsi.responsibleforcleanlinesstoiletsboys,
    responsibleforcleanlinesstoiletsgirls = bsi.responsibleforcleanlinesstoiletsgirls,
    toilettypeftconnmainsewerline = bsi.toilettypeftconnmainsewerline,
    toilettypeftconnseptictank = bsi.toilettypeftconnseptictank,
    toilettypeftconnpit = bsi.toilettypeftconnpit,
    toilettypeftconnanotherloc = bsi.toilettypeftconnanotherloc,
    toilettypevippitlatrine = bsi.toilettypevippitlatrine,
    toilettypepitlatrinewithfloor = bsi.toilettypepitlatrinewithfloor,
    toilettypeopenpitlatrine = bsi.toilettypeopenpitlatrine,
    toilettypeothers = bsi.toilettypeothers,
    burningchambers = bsi.burningchambers,
    septictanks = bsi.septictanks,
    watertanks = bsi.watertanks,
    specialgirlsroom = bsi.specialgirlsroom
  FROM public.bemis_school_infrastructure bsi
  WHERE bscv.schoolregnumber = bsi.schoolregnumber;

SELECT count(*) from visualization.bemis_school_comb_vis 
-- 26,582 in both 
----------------------------------------------------------------------------------------------------------------------
-- add the number of pupils from the enrollment TABLE
ALTER TABLE visualization.bemis_school_comb_vis  
  ADD COLUMN total_pupils VARCHAR,
  ADD COLUMN girl_pupils VARCHAR(255),
  ADD COLUMN boy_pupils VARCHAR,

  
UPDATE visualization.bemis_school_comb_vis  AS sch
  SET 
    total_pupils = e.totalpupils,
    girl_pupils = e.girl_pupils,
    boy_pupils = e.boy_pupils,
  FROM public.bemis_school_enrollment  AS e
  WHERE sch.schoolregnumber = e.schoolregnumber ;
  
SELECT * from visualization.bemis_school_comb_vis LIMIT 100; 


----------------------------------------------------------------------------------------------------------------------
--3. Create additional variables for visualization
----------------------------------------------------------------------------------------------------------------------

-- create a variable that separates out schoolregnumber as primary/pre-primary or secondary 
ALTER TABLE visualization.bemis_school_comb_vis 
ADD COLUMN school_category TEXT;

UPDATE visualization.bemis_school_comb_vis 
SET school_category = 
    CASE 
        WHEN SPLIT_PART(schoolregnumber, '.', 1) IN ('EM', 'ES') THEN 'Primary/Pre-Primary'
        WHEN SPLIT_PART(schoolregnumber, '.', 1) = 'S' THEN 'Secondary'
        ELSE 'Unknown'
    END;

----------------------------------------------------------------------------------------------------------------------
-- create improved water source categories 
ALTER TABLE visualization.bemis_school_comb_vis 
ADD COLUMN improved_water_source BOOLEAN;

-- first check types 
SELECT 
    watersource, 
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM visualization.bemis_school_comb_vis
GROUP BY watersource
ORDER BY count DESC;

-- just check nulls in watersource
SELECT COUNT(*) AS null_count
FROM visualization.bemis_school_comb_vis 
WHERE watersource IS NULL;
-- 603 schools (likely same as those missing from infrastructure)
UPDATE visualization.bemis_school_comb_vis
SET improved_water_source = 
    CASE 
      WHEN watersource ILIKE '%default' 
             OR watersource ILIKE '%maji ya bomba%' 
             -- piped water
             OR watersource ILIKE '%maji ya mvua%' 
             -- rainwater
             OR watersource ILIKE '%visima vifupi%'
             -- shallow well 
             OR watersource ILIKE '%visima vilivyojengwa%'
             -- constructed well
             OR watersource ILIKE '%visima vifupi vilivyofungwa pampu za mikono%'
             -- shallow well with handpump
             OR watersource ILIKE '%visima virefu vilivyofungwa pampu za mikono%'
        THEN TRUE 
    
        WHEN watersource ILIKE '%maji ya mto%' 
            -- river water 
             OR watersource ILIKE '%bwawa%' 
             -- dam
             OR watersource ILIKE '%maji ya ziwa%'
             -- lake water 
             OR watersource ILIKE '%chemchem%'
             -- spring
             OR watersource ILIKE '%vyanzo vingine%'
            THEN FALSE 
            ELSE NULL 
    END;    

-- check distribution 
SELECT 
    improved_water_source, 
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM visualization.bemis_school_comb_vis
GROUP BY improved_water_source
ORDER BY count DESC;

-- check remaining uncoded sources
WITH total AS (
    SELECT COUNT(*) AS total_count
    FROM visualization.bemis_school_comb_vis
    WHERE improved_water_source IS NULL
)
SELECT 
    watersource, 
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / total.total_count, 2) AS percentage
FROM visualization.bemis_school_comb_vis, total
WHERE improved_water_source IS NULL
GROUP BY watersource, total.total_count
ORDER BY count DESC;

----------------------------------------------------------------------------------------------------------------------
--- Improved toilet classification part (a) Facility type 
--- includes: flush/pour flush toilets connected to piped sewer systems, septic tanks or pit latrines; pit latrines with slabs (including ventilated pit latrines), and composting toilets. 

ALTER TABLE visualization.bemis_school_comb_vis 
    ADD COLUMN improved_toilet_type BOOLEAN;

UPDATE visualization.bemis_school_comb_vis 
  SET improved_toilet_type = TRUE 
  WHERE toilettypeftconnmainsewerline =TRUE
    OR toilettypeftconnseptictank = TRUE 
    OR toilettypeftconnpit = TRUE  
    OR toilettypevippitlatrine = TRUE;
    
UPDATE visualization.bemis_school_comb_vis 
  SET improved_toilet_type = FALSE 
  WHERE toilettypeftconnanotherloc =TRUE
    OR toilettypepitlatrinewithfloor = TRUE 
    OR toilettypeopenpitlatrine = TRUE  
    OR toilettypeothers = TRUE;

-- check stats 
SELECT 
    improved_toilet_type, 
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM visualization.bemis_school_comb_vis
GROUP BY bemis_school_comb_vis.improved_toilet_type
ORDER BY count DESC;

----------------------------------------------------------------------------------------------------------------------
-- part (b) -- Provide one drop hole per 40 girls, one drop hole per 50 boys and one drop hole suitable for disabled pupils.
-- In schools with an enrolment above 1,500 students, a student to drop hole ratio of one drop hole per 50 girls and one drop hole per 65 for boys can be accepted.

ALTER TABLE visualization.bemis_school_comb_vis
ADD COLUMN girls_per_drophole NUMERIC,
    ADD COLUMN boys_per_drophole NUMERIC,
    ADD COLUMN enrollment_over1500 BOOLEAN,
    ADD COLUMN meet_drophole_ratio BOOLEAN,
    ADD COLUMN teacher_toilet_block BOOLEAN,
    ADD COLUMN drophole_specialneeds BOOLEAN;

UPDATE visualization.bemis_school_comb_vis 
SET 
    girls_per_drophole = CASE WHEN dropholesgirls = 0 THEN NULL ELSE girl_pupils / dropholesgirls END,
    boys_per_drophole = CASE WHEN dropholesboys = 0 THEN NULL ELSE boy_pupils / dropholesboys END,
    drophole_specialneeds = CASE WHEN dropholesspecialneedsboys > 0 OR dropholesspecialneedsgirls > 0 
                            THEN TRUE ELSE drophole_specialneeds END,
    enrollment_over1500 = CASE WHEN totalpupils >= 1500 AND totalpupils IS NOT NULL THEN TRUE 
                               WHEN totalpupils < 1500 AND totalpupils IS NOT NULL THEN FALSE 
                               ELSE NULL END,
    meet_drophole_ratio = CASE 
                            WHEN girls_per_drophole <= 40 AND boys_per_drophole <= 50 AND drophole_specialneeds = TRUE AND enrollment_over1500 = FALSE 
                            THEN TRUE
                            WHEN girls_per_drophole <= 50 AND boys_per_drophole <= 65 AND drophole_specialneeds = TRUE AND enrollment_over1500 = TRUE 
                            THEN TRUE
                            ELSE FALSE END;

----------------------------------------------------------------------------------------------------------------------
-- part (c) -- There are separate latrine blocks for girls and boys and a segregated block of latrines for teachers.
-- no variable for separate blocks but there is one for teachers 

ALTER TABLE visualization.bemis_school_comb_vis
ADD COLUMN separate_latrines_teachers BOOLEAN;

UPDATE visualization.bemis_school_comb_vis
SET separate_latrines_teachers = 
    CASE 
        WHEN dropholesmaleteachers > 0 AND dropholesfemaleteachers > 0 
        AND dropholesmaleteachers IS NOT NULL AND dropholesfemaleteachers IS NOT NULL 
        THEN TRUE
        WHEN dropholesmaleteachers IS NULL AND dropholesfemaleteachers IS NULL
        THEN NULL 
        ELSE FALSE 
    END;

----------------------------------------------------------------------------------------------------------------------
-- part (d) create improved sanitation coverage variable on the basis of all these conditions 
ALTER TABLE visualization.bemis_school_comb_vis 
    ADD COLUMN improved_school_sanitation BOOLEAN;

UPDATE visualization.bemis_school_comb_vis
  SET improved_school_sanitation =
    CASE  
      WHEN improved_school_sanitation = TRUE 
      AND meet_drophole_ratio = TRUE
      THEN TRUE 
      ELSE FALSE 
    END;

--- checking where there are no toilet types specified in this question - output this list and send it to Shaban 
SELECT 
    schoolregnumber,
    toilettypeftconnmainsewerline, 
    toilettypeftconnseptictank, 
    toilettypeftconnpit, 
    toilettypeftconnanotherloc, 
    toilettypevippitlatrine, 
    toilettypepitlatrinewithfloor, 
    toilettypeopenpitlatrine, 
    toilettypeothers
FROM public.bemis_school_infrastructure
WHERE 
    toilettypeftconnmainsewerline IS FALSE
    AND toilettypeftconnseptictank IS FALSE
    AND toilettypeftconnpit IS FALSE
    AND toilettypeftconnanotherloc IS FALSE
    AND toilettypevippitlatrine IS FALSE
    AND toilettypepitlatrinewithfloor IS FALSE
    AND toilettypeopenpitlatrine IS FALSE
    AND toilettypeothers IS FALSE;


SELECT COUNT(schoolregnumber) AS count
FROM visualization.bemis_school_comb_vis
WHERE toilettypeftconnmainsewerline IS FALSE
    AND toilettypeftconnseptictank IS FALSE
    AND toilettypeftconnpit IS FALSE
    AND toilettypeftconnanotherloc IS FALSE
    AND toilettypevippitlatrine IS FALSE
    AND toilettypepitlatrinewithfloor IS FALSE
    AND toilettypeopenpitlatrine IS FALSE
    AND toilettypeothers IS FALSE;
-- coding worked correctly so a lot of data is missing 





