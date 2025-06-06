

-- ============================================================================
-- process_bemis_data: Build visualization.bemis_school_comb_vis for reporting
--
-- This procedure creates and populates the visualization.bemis_school_comb_vis table
-- by combining and enriching data from BEMIS source tables and reference lookups.
--
-- DEPENDENCIES (must exist and be fully populated BEFORE running):
--   * public.bemis_school_services
--   * public.bemis_school_reports
--   * public.bemis_school_infrastructure
--   * public.bemis_school_enrollment
--   * visualization.region_district_lga_names
--     (run process_region_district_lga_names if region/LGA reference data changed)
--
-- RECOMMENDED EXECUTION ORDER:
--   1. Ensure all public.bemis_* tables are loaded and current (external ETL/import)
--   2. Run process_region_district_lga_names (if needed)
--   3. Run this procedure (process_bemis_data)
--
-- NOTE: If any dependency is missing or stale, output will be incomplete or incorrect.
--       This script is typically run yearly after new data is received from PO-RALG.
-- ============================================================================

-- ============================================================================
-- QC LOGGING TABLE (create once, outside this procedure):
--
-- CREATE SCHEMA IF NOT EXISTS quality_checks;
-- CREATE TABLE IF NOT EXISTS quality_checks.qc_log (
--     id SERIAL PRIMARY KEY,
--     check_name TEXT,
--     result_value TEXT,
--     details TEXT,
--     log_time TIMESTAMP DEFAULT NOW()
-- );
-- ============================================================================

CREATE OR REPLACE PROCEDURE public.process_bemis_data()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1.1: Drop Existing Combined Table if It Exists
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.bemis_school_comb_vis';

    --------------------------------------------------------------------------
    -- Step 1.2: Create the New Combined Table with All Required Columns
    --------------------------------------------------------------------------
    EXECUTE '
    CREATE TABLE visualization.bemis_school_comb_vis (
        schoolregnumber VARCHAR PRIMARY KEY,
        reportdate      DATE,
        watersource     VARCHAR,
        region_code     VARCHAR,
        lga_code        VARCHAR(255),
        gps_coordinates VARCHAR,
        schoolpforrprogram BOOLEAN,
        region_name     VARCHAR,
        lga_name        VARCHAR(255),
        classroomcount  INT,
        classroomsrequired INT,
        dispensaries    INT,
        dropholesboys   INT,
        dropholesgirls  INT,
        dropholesspecialneedsboys INT,
        dropholesspecialneedsgirls INT,
        dropholesmaleteachers INT,
        dropholesfemaleteachers INT,
        latrineslockable BOOLEAN,
        latrinesmaintained BOOLEAN,
        latrineshavewashablefloor BOOLEAN,
        latrineswithoutwastewaterdischarge BOOLEAN,
        latrinesconnectedstspcesspit BOOLEAN,
        responsibleforcleanlinesstoiletsboys VARCHAR,
        responsibleforcleanlinesstoiletsgirls VARCHAR,
        toilettypeftconnmainsewerline BOOLEAN,
        toilettypeftconnseptictank BOOLEAN,
        toilettypeftconnpit BOOLEAN,
        toilettypeftconnanotherloc BOOLEAN,
        toilettypevippitlatrine BOOLEAN,
        toilettypepitlatrinewithfloor BOOLEAN,
        toilettypeopenpitlatrine BOOLEAN,
        toilettypeothers BOOLEAN,
        burningchambers  INT,
        septictanks      INT,
        watertanks       INT,
        specialgirlsroom BOOLEAN,
        school_category  TEXT,
        improved_water_source NUMERIC,
        improved_toilet_type NUMERIC,
        separate_latrines_teachers BOOLEAN,
        total_pupils NUMERIC,
        girl_pupils NUMERIC,
        boy_pupils NUMERIC,
        girls_per_drophole NUMERIC,
        boys_per_drophole NUMERIC,
        enrollment_over1500 BOOLEAN,
        meet_drophole_ratio BOOLEAN,
        teacher_toilet_block BOOLEAN,
        drophole_specialneeds BOOLEAN,
        improved_sanitation_school NUMERIC,
        haselectricity BOOLEAN,
        handwashingfacilities INTEGER,
        handwashingtype BOOLEAN,
        electricitysource TEXT,
        sanitarypadsprovided BOOLEAN,
        menstrualcounselor BOOLEAN,
        capitationgrant NUMERIC,
        signedsitebook BOOLEAN,
        reportdate DATE,
        completioncertificate BOOLEAN
    )';
  
    --------------------------------------------------------------------------
    -- Step 2: Insert Base Data from bemis_school_services
    --------------------------------------------------------------------------
    -- WORKAROUND: Only include records with reportdate before 2025 to avoid duplicate schoolregnumber issues.
    -- This is a temporary solution and should be revisited to properly handle multiple years of data.
    
INSERT INTO visualization.bemis_school_comb_vis(schoolregnumber,
                    reportdate,
                    haselectricity,
                    handwashingfacilities,
                    handwashingtype,
                    watersource,
                    electricitysource,
                    sanitarypadsprovided,
                    menstrualcounselor,
                    capitationgrant,
                    signedsitebook,
                    reportdate,
                    completioncertificate)
      SELECT
                    schoolregnumber,
                    reportdate,
                    haselectricity,
                    handwashingfacilities,
                    handwashingtype,
                    watersource,
                    electricitysource,
                    sanitarypadsprovided,
                    menstrualcounselor,
                    capitationgrant,
                    signedsitebook,
                    reportdate,
                    completioncertificate
      FROM public.bemis_school_services
      WHERE EXTRACT(YEAR FROM bemis_school_services.reportdate) = 2024;
    ---TODO: note this is a temporary fix which should be changed so we can just see 2024 data

    -- QC: Step 2 - Unique schools in services
    INSERT INTO quality_checks.qc_log (check_name, result_value)
    SELECT 'unique_schools_services', COUNT(DISTINCT schoolregnumber)::TEXT
    FROM public.bemis_school_services
    WHERE EXTRACT(YEAR FROM reportdate) = 2024;

    --------------------------------------------------------------------------
    -- Step 3: Update with Region Codes, GPS Coordinates, and Program Flag 
    --         from bemis_school_reports
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis AS sch
    SET 
      region_code = r.regioncode,
      lga_code = r.lgacode,
      gps_coordinates = r.gpscoordinates,
      schoolpforrprogram = r.schoolpforrprogram
    FROM public.bemis_school_reports AS r
    WHERE sch.schoolregnumber = r.schoolregnumber
    ';

    -- QC: Step 3 - Unique in reports
    INSERT INTO quality_checks.qc_log (check_name, result_value)
    SELECT 'unique_schools_reports', COUNT(DISTINCT schoolregnumber)::TEXT
    FROM public.bemis_school_reports;

    -- QC: Step 3 - Matched between services and reports
    INSERT INTO quality_checks.qc_log (check_name, result_value)
    SELECT 'matched_services_reports', COUNT(DISTINCT s.schoolregnumber)::TEXT
    FROM public.bemis_school_services s
    JOIN public.bemis_school_reports r ON s.schoolregnumber = r.schoolregnumber
    WHERE EXTRACT(YEAR FROM s.reportdate) = 2024;

    -- QC: Step 3 - In services but not in reports
    INSERT INTO quality_checks.qc_log (check_name, result_value, details)
    SELECT 'unmatched_services_not_in_reports', COUNT(*), STRING_AGG(unmatched.schoolregnumber, ', ')
    FROM (
        SELECT s.schoolregnumber
        FROM public.bemis_school_services s
        LEFT JOIN public.bemis_school_reports r ON s.schoolregnumber = r.schoolregnumber
        WHERE r.schoolregnumber IS NULL AND EXTRACT(YEAR FROM s.reportdate) = 2024
    ) unmatched;

    -- QC: Step 3 - In reports but not in services
    INSERT INTO quality_checks.qc_log (check_name, result_value, details)
    SELECT 'unmatched_reports_not_in_services', COUNT(*), STRING_AGG(unmatched.schoolregnumber, ', ')
    FROM (
        SELECT r.schoolregnumber
        FROM public.bemis_school_reports r
        LEFT JOIN public.bemis_school_services s ON r.schoolregnumber = s.schoolregnumber
        WHERE s.schoolregnumber IS NULL
    ) unmatched;

    --------------------------------------------------------------------------
    -- Step 4: Update with Region and LGA Names from the Lookup Table
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis AS sch
    SET 
      region_name = n.region_name,
      lga_name = n.lga_name
    FROM visualization.region_district_lga_names AS n
    WHERE sch.lga_code = n.bemislgacode
    ';

    -- QC: Step 4 - Schools missing region/LGA names
    INSERT INTO quality_checks.qc_log (check_name, result_value, details)
    SELECT 'missing_region_lga_names', COUNT(*), STRING_AGG(schoolregnumber, ', ')
    FROM visualization.bemis_school_comb_vis
    WHERE region_name IS NULL OR lga_name IS NULL;

    --------------------------------------------------------------------------
    -- Step 5: Update Infrastructure Details from bemis_school_infrastructure
    --------------------------------------------------------------------------
    EXECUTE '
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
    FROM public.bemis_school_infrastructure AS bsi
    WHERE bscv.schoolregnumber = bsi.schoolregnumber AND
    EXTRACT(YEAR FROM bsi.reportdate) = 2024;
    ';
    -- TODO: remove temporary fix to check out just one year at a time in the data 
    -- QC: check and output the number of unique school observations in bemis_school_infrastructure, and those that match or are unmatched with table created so far (bemis_school_comb_vis) (column for number matched, column for number unmatched from master to using, and visa versa)
    -- QC: if there are unmatched that appear in school_infrastructure but not master then put in description column, likewise for those in the master but not in infrastructure

    -- QC: Step 5 - Unique in infrastructure
    INSERT INTO quality_checks.qc_log (check_name, result_value)
    SELECT 'unique_schools_infrastructure', COUNT(DISTINCT schoolregnumber)::TEXT
    FROM public.bemis_school_infrastructure;

    -- QC: Step 5 - Matched between combined and infrastructure
    INSERT INTO quality_checks.qc_log (check_name, result_value)
    SELECT 'matched_combined_infrastructure', COUNT(DISTINCT bscv.schoolregnumber)::TEXT
    FROM visualization.bemis_school_comb_vis bscv
    JOIN public.bemis_school_infrastructure bsi ON bscv.schoolregnumber = bsi.schoolregnumber;

    -- QC: Step 5 - In infrastructure but not in combined
    INSERT INTO quality_checks.qc_log (check_name, result_value, details)
    SELECT 'infra_not_in_combined', COUNT(*), STRING_AGG(unmatched.schoolregnumber, ', ')
    FROM (
        SELECT bsi.schoolregnumber
        FROM public.bemis_school_infrastructure bsi
        LEFT JOIN visualization.bemis_school_comb_vis bscv ON bsi.schoolregnumber = bscv.schoolregnumber
        WHERE bscv.schoolregnumber IS NULL
    ) unmatched;

    -- QC: Step 5 - In combined but not in infrastructure
    INSERT INTO quality_checks.qc_log (check_name, result_value, details)
    SELECT 'combined_not_in_infra', COUNT(*), STRING_AGG(unmatched.schoolregnumber, ', ')
    FROM (
        SELECT bscv.schoolregnumber
        FROM visualization.bemis_school_comb_vis bscv
        LEFT JOIN public.bemis_school_infrastructure bsi ON bscv.schoolregnumber = bsi.schoolregnumber
        WHERE bsi.schoolregnumber IS NULL
    ) unmatched;

    --------------------------------------------------------------------------
    -- Step 6: Derive Additional Visualization Variables
    -- 6a: Update school_category based on registration number prefix
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis
    SET school_category = CASE 
          WHEN SPLIT_PART(schoolregnumber, ''.'', 1) IN (''EM'', ''ES'') THEN ''Primary/Pre-Primary''
          WHEN SPLIT_PART(schoolregnumber, ''.'', 1) = ''S'' THEN ''Secondary''
          ELSE ''Unknown''
       END
    ';
    -- QC: Step 6a - Count by school category
    INSERT INTO quality_checks.qc_log (check_name, result_value, details)
    SELECT 'school_category_counts', NULL, STRING_AGG(school_category || ':' || count, ', ')
    FROM (
        SELECT school_category, COUNT(*) AS count
        FROM visualization.bemis_school_comb_vis
        GROUP BY school_category
    ) t;

    --------------------------------------------------------------------------
    -- 6b: Categorize improved_water_source based on text patterns in watersource
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis
          SET improved_water_source = CASE 
            WHEN watersource ILIKE ANY (ARRAY[
                '%default%',
                '%maji ya bomba%',
                '%maji ya mvua%',
                '%visima vifupi%',
                '%visima vilivyojengwa%',
                '%visima vifupi vilivyofungwa pampu za mikono%'
            ]) THEN 1

            WHEN watersource ILIKE ANY (ARRAY[
                '%maji ya mto%',
                '%bwawa%',
                '%maji ya ziwa%',
                '%chemchem%'
            ]) THEN 0

            ELSE NULL
          END';
 
  -- QC: Step 6b - Count NULL watersource
  INSERT INTO quality_checks.qc_log (check_name, result_value)
  SELECT 'null_watersource_count', COUNT(*)::TEXT
  FROM visualization.bemis_school_comb_vis
  WHERE watersource IS NULL;

    --------------------------------------------------------------------------
    -- 6c: Set improved_toilet_type based on infrastructure flags
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis 
    SET improved_toilet_type = 1 
    WHERE toilettypeftconnmainsewerline = TRUE
       OR toilettypeftconnseptictank = TRUE 
       OR toilettypeftconnpit = TRUE  
       OR toilettypevippitlatrine = TRUE
    ';
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis 
    SET improved_toilet_type = 0 
    WHERE toilettypeftconnanotherloc = TRUE
       OR toilettypepitlatrinewithfloor = TRUE 
       OR toilettypeopenpitlatrine = TRUE  
       OR toilettypeothers = TRUE
    ';
  -- QC: Step 6c - Count NULL improved_toilet_type
  INSERT INTO quality_checks.qc_log (check_name, result_value)
  SELECT 'null_improved_toilet_type', COUNT(*)::TEXT
  FROM visualization.bemis_school_comb_vis
  WHERE improved_toilet_type IS NULL;

    --------------------------------------------------------------------------
    -- Step 7: Final Adjustments – Set Defaults for Latrine Fields
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis
    SET latrineslockable = TRUE
    WHERE latrinesmaintained = TRUE 
       OR specialgirlsroom = TRUE 
       OR schoolregnumber LIKE ''EM%''
    ';
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis
    SET latrineslockable = COALESCE(latrineslockable, FALSE),
        specialgirlsroom = COALESCE(specialgirlsroom, FALSE),
        latrinesmaintained = COALESCE(latrinesmaintained, FALSE)
    WHERE schoolregnumber IS NOT NULL
    ';

    --------------------------------------------------------------------------
    -- Step 8: Add and Update Pupil Count Columns from bemis_school_enrollment
    --------------------------------------------------------------------------
  
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis AS sch
    SET total_pupils = e.totalpupils::NUMERIC,
        girl_pupils = e.totalgirls::NUMERIC,
        boy_pupils = e.totalboys::NUMERIC
    FROM public.bemis_school_enrollment AS e
    WHERE sch.schoolregnumber = e.schoolregnumber
    ';
    -- QC: Step 8 - Totals for boys/girls/total
    INSERT INTO quality_checks.qc_log (check_name, result_value, details)
    SELECT 'enrollment_totals', NULL, 'boys:' || COALESCE(SUM(boy_pupils),0) || ', girls:' || COALESCE(SUM(girl_pupils),0) || ', total:' || COALESCE(SUM(total_pupils),0)
    FROM visualization.bemis_school_comb_vis;

    --------------------------------------------------------------------------
    -- Step 9: Update Columns for Drop Hole Ratios and Related Flags
    --------------------------------------------------------------------------
    
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis 
    SET 
      girls_per_drophole = CASE 
          WHEN dropholesgirls IS NULL OR dropholesgirls = 0 THEN NULL  
          ELSE girl_pupils::NUMERIC / dropholesgirls 
      END,
      boys_per_drophole = CASE 
          WHEN dropholesboys IS NULL OR dropholesboys = 0 THEN NULL  
          ELSE boy_pupils::NUMERIC / dropholesboys 
      END,
      drophole_specialneeds = CASE 
          WHEN dropholesspecialneedsboys > 0 OR dropholesspecialneedsgirls > 0 THEN TRUE 
          ELSE FALSE 
      END,
      enrollment_over1500 = CASE 
          WHEN total_pupils IS NULL THEN NULL  
          WHEN total_pupils >= 1500 THEN TRUE 
          ELSE FALSE 
      END
    ';
    -- QC: Step 9 - Over/under 1500 pupils
    INSERT INTO quality_checks.qc_log (check_name, result_value, details)
    SELECT 'enrollment_over_under_1500', NULL, 'over_1500:' || SUM(CASE WHEN total_pupils >= 1500 THEN 1 ELSE 0 END) || ', under_1500:' || SUM(CASE WHEN total_pupils < 1500 THEN 1 ELSE 0 END)
    FROM visualization.bemis_school_comb_vis;

    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis 
    SET meet_drophole_ratio = CASE 
          WHEN girls_per_drophole <= 40 AND boys_per_drophole <= 50 
               AND drophole_specialneeds = TRUE AND enrollment_over1500 = FALSE 
            THEN TRUE 
          WHEN girls_per_drophole <= 50 AND boys_per_drophole <= 65 
               AND drophole_specialneeds = TRUE AND enrollment_over1500 = TRUE 
            THEN TRUE 
          ELSE FALSE 
       END
    ';
  -- QC: Step 9 - Count meeting drophole ratio
  INSERT INTO quality_checks.qc_log (check_name, result_value)
  SELECT 'meet_drophole_ratio_count', COUNT(*)::TEXT
  FROM visualization.bemis_school_comb_vis
  WHERE meet_drophole_ratio = TRUE;

    --------------------------------------------------------------------------
    -- Step 10: Update Column for Separate Latrines for Teachers (if not exists)
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis
    SET separate_latrines_teachers = CASE 
          WHEN dropholesmaleteachers > 0 AND dropholesfemaleteachers > 0 THEN TRUE
          WHEN dropholesmaleteachers IS NULL OR dropholesfemaleteachers IS NULL THEN NULL 
          ELSE FALSE
       END
    ';

    -- QC: Step 10 - Count with separate latrines for teachers
    INSERT INTO quality_checks.qc_log (check_name, result_value)
    SELECT 'separate_latrines_teachers_count', COUNT(*)::TEXT
    FROM visualization.bemis_school_comb_vis
    WHERE separate_latrines_teachers = TRUE;

    --------------------------------------------------------------------------
    -- Step 10: Update column for improved sanitation
    --------------------------------------------------------------------------

    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis 
    SET improved_sanitation_school = 
      CASE 
        WHEN improved_toilet_type IS NULL 
            OR meet_drophole_ratio IS NULL 
            OR separate_latrines_teachers IS NULL
        THEN NULL  
        WHEN improved_toilet_type = 1 
            AND meet_drophole_ratio IS TRUE 
            AND separate_latrines_teachers IS TRUE
        THEN 1
        ELSE 0 
      END
      ';
    -- QC: Step 10 - Percent improved sanitation
    INSERT INTO quality_checks.qc_log (check_name, result_value)
    SELECT 'percent_improved_sanitation',
        (100.0 * SUM(CASE WHEN improved_sanitation_school = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0))::TEXT
    FROM visualization.bemis_school_comb_vis;

    -- QC: Step 8 - Mismatches
    INSERT INTO quality_checks.qc_log (check_name, result_value, details)
    SELECT 'enrollment_mismatches', COUNT(*), STRING_AGG(unmatched.schoolregnumber, ', ')
    FROM (
        SELECT schoolregnumber
        FROM visualization.bemis_school_comb_vis
        WHERE boy_pupils + girl_pupils != total_pupils
    ) unmatched;
END;
$$;