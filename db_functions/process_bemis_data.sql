
CREATE OR REPLACE PROCEDURE process_bemis_data()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Drop Existing Combined Table if It Exists
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.bemis_school_comb_vis';

    --------------------------------------------------------------------------
    -- Step 2: Create the Combined Table with All Required Columns
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
        classroomcount                   INT,
        classroomsrequired               INT,
        dispensaries                     INT,
        dropholesboys                    INT,
        dropholesgirls                   INT,
        dropholesspecialneedsboys        INT,
        dropholesspecialneedsgirls       INT,
        dropholesmaleteachers            INT,
        dropholesfemaleteachers          INT,
        latrineslockable                 BOOLEAN,
        latrinesmaintained               BOOLEAN,
        latrineshavewashablefloor        BOOLEAN,
        latrineswithoutwastewaterdischarge BOOLEAN,
        latrinesconnectedstspcesspit     BOOLEAN,
        responsibleforcleanlinesstoiletsboys VARCHAR,
        responsibleforcleanlinesstoiletsgirls VARCHAR,
        toilettypeftconnmainsewerline    BOOLEAN,
        toilettypeftconnseptictank       BOOLEAN,
        toilettypeftconnpit              BOOLEAN,
        toilettypeftconnanotherloc       BOOLEAN,
        toilettypevippitlatrine          BOOLEAN,
        toilettypepitlatrinewithfloor    BOOLEAN,
        toilettypeopenpitlatrine         BOOLEAN,
        toilettypeothers                 BOOLEAN,
        burningchambers                  INT,
        septictanks                      INT,
        watertanks                       INT,
        specialgirlsroom                 BOOLEAN,
        school_category     TEXT,
        improved_water_source BOOLEAN,
        girls_per_drophole  NUMERIC,
        boys_per_drophole   NUMERIC,
        enrollment_over1500 NUMERIC,
        meet_drophole_ratio BOOLEAN,
        teacher_toilet_block BOOLEAN,
        improved_toilet_type BOOLEAN
    )';

    --------------------------------------------------------------------------
    -- Step 3: Insert Base Data from bemis_school_services into the Combined Table
    --------------------------------------------------------------------------
    EXECUTE '
    INSERT INTO visualization.bemis_school_comb_vis (schoolregnumber, watersource)
    SELECT schoolregnumber, watersource
    FROM public.bemis_school_services
    ';

    --------------------------------------------------------------------------
    -- Step 4: Update Region Codes, GPS Coordinates, and Program Flag from bemis_school_reports
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
    -- Step 5: Update Region and LGA Names Using the Lookup Table (region_district_lga_names)
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
    -- Step 6: Update Infrastructure Details from bemis_school_infrastructure
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
    -- Step 7: Derive Additional Visualization Variables
    --------------------------------------------------------------------------
    -- 7a. Update school category based on registration number prefix.
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis
    SET school_category = 
        CASE 
            WHEN SPLIT_PART(schoolregnumber, ''.'', 1) IN (''EM'', ''ES'') THEN ''Primary/Pre-Primary''
            WHEN SPLIT_PART(schoolregnumber, ''.'', 1) = ''S'' THEN ''Secondary''
            ELSE ''Unknown''
        END
    ';

    -- 7b. Categorize improved water source based on text patterns in the watersource.
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis
    SET improved_water_source = 
        CASE 
          WHEN watersource ILIKE ''%default%'' 
               OR watersource ILIKE ''%maji ya bomba%'' 
               OR watersource ILIKE ''%maji ya mvua%'' 
               OR watersource ILIKE ''%visima vifupi%'' 
               OR watersource ILIKE ''%visima vilivyojengwa%'' 
               OR watersource ILIKE ''%visima vifupi vilivyofungwa pampu za mikono%'' 
            THEN TRUE 
          WHEN watersource ILIKE ''%maji ya mto%'' 
               OR watersource ILIKE ''%bwawa%'' 
               OR watersource ILIKE ''%maji ya ziwa%'' 
               OR watersource ILIKE ''%chemchem%'' 
            THEN FALSE 
          ELSE NULL 
        END
    ';

    -- 7c. Set improved toilet type based on infrastructure flags.
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis 
      SET improved_toilet_type = TRUE 
      WHERE toilettypeftconnmainsewerline = TRUE
         OR toilettypeftconnseptictank = TRUE 
         OR toilettypeftconnpit = TRUE  
         OR toilettypevippitlatrine = TRUE
    ';
    
    EXECUTE '
    UPDATE visualization.bemis_school_comb_vis 
      SET improved_toilet_type = FALSE 
      WHERE toilettypeftconnanotherloc = TRUE
         OR toilettypepitlatrinewithfloor = TRUE 
         OR toilettypeopenpitlatrine = TRUE  
         OR toilettypeothers = TRUE
    ';

    --------------------------------------------------------------------------
    -- Step 8: Final Adjustments – Set Default Values for Nulls and Update Derived Flags
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
    SET 
        latrineslockable = COALESCE(latrineslockable, FALSE),
        specialgirlsroom = COALESCE(specialgirlsroom, FALSE),
        latrinesmaintained = COALESCE(latrinesmaintained, FALSE)
    WHERE schoolregnumber IS NOT NULL
    ';
    
END;
$$;



