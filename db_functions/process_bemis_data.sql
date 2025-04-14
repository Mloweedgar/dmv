--------------------------------------------------------------------------
-- Create or Replace Procedure to Process BEMIS Data for Visualization
-- This procedure creates a combined table and enriches it using data from 
-- various raw sources (services, reports, infrastructure, enrollment, etc.).
-- Recommendation: Schedule for this script can be yearly because we receive data from PO-RALG yearly
--------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE process_bemis_data()
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
        completioncertificate BOOLEAN
    )';

    --------------------------------------------------------------------------
    -- Step 2: Insert Base Data from bemis_school_services
    --------------------------------------------------------------------------
   
    
INSERT INTO visualization.bemis_school_comb_vis(schoolregnumber,
                    haselectricity,
                    handwashingfacilities,
                    handwashingtype,
                    watersource,
                    electricitysource,
                    sanitarypadsprovided,
                    menstrualcounselor,
                    capitationgrant,
                    signedsitebook,
                    completioncertificate)
      SELECT
                    schoolregnumber,
                    haselectricity,
                    handwashingfacilities,
                    handwashingtype,
                    watersource,
                    electricitysource,
                    sanitarypadsprovided,
                    menstrualcounselor,
                    capitationgrant,
                    signedsitebook,
                    completioncertificate
      FROM public.bemis_school_services;

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
    WHERE bscv.schoolregnumber = bsi.schoolregnumber
    ';

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

    --------------------------------------------------------------------------
    -- 6b: Categorize improved_water_source based on text patterns in watersource
    --------------------------------------------------------------------------
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis
    SET improved_water_source = CASE 
          WHEN watersource ILIKE ''%default%'' 
            OR watersource ILIKE ''%maji ya bomba%'' 
            OR watersource ILIKE ''%maji ya mvua%'' 
            OR watersource ILIKE ''%visima vifupi%'' 
            OR watersource ILIKE ''%visima vilivyojengwa%'' 
            OR watersource ILIKE ''%visima vifupi vilivyofungwa pampu za mikono%'' 
          THEN 1 
          WHEN watersource ILIKE ''%maji ya mto%'' 
            OR watersource ILIKE ''%bwawa%'' 
            OR watersource ILIKE ''%maji ya ziwa%'' 
            OR watersource ILIKE ''%chemchem%'' 
          THEN 0 
          ELSE NULL 
       END
    ';

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

END;
$$;
