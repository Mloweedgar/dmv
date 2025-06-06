CREATE OR REPLACE PROCEDURE public.process_bemis_quality_checks()
LANGUAGE plpgsql
AS $$
BEGIN
--------------------------------------------------------------------------
--- Code that needs adding as follows: 
--------------------------------------------------------------------------

-- check number of school observations in each of the public databases before joining 
-- print out a report on number of obs in each and send if above or below a certain threshold 
-- tabulate null values for key variables as a percentage of the number of schools in the largest number in the database specifically: 
---classroomcount = bsi.classroomcount,
    --   classroomsrequired = bsi.classroomsrequired,
    --   dispensaries = bsi.dispensaries,
    --   dropholesboys = bsi.dropholesboys,
    --   dropholesgirls = bsi.dropholesgirls,
    --   dropholesspecialneedsboys = bsi.dropholesspecialneedsboys,
    --   dropholesspecialneedsgirls = bsi.dropholesspecialneedsgirls,
    --   dropholesmaleteachers = bsi.dropholesmaleteachers,
    --   dropholesfemaleteachers = bsi.dropholesfemaleteachers,
    --   latrineslockable = bsi.latrineslockable,
    --   latrinesmaintained = bsi.latrinesmaintained,
    --   latrineshavewashablefloor = bsi.latrineshavewashablefloor,
    --   latrineswithoutwastewaterdischarge = bsi.latrineswithoutwastewaterdischarge,
    --   latrinesconnectedstspcesspit = bsi.latrinesconnectedstspcesspit,
    --   responsibleforcleanlinesstoiletsboys = bsi.responsibleforcleanlinesstoiletsboys,
    --   responsibleforcleanlinesstoiletsgirls = bsi.responsibleforcleanlinesstoiletsgirls,
    --   toilettypeftconnmainsewerline = bsi.toilettypeftconnmainsewerline,
    --   toilettypeftconnseptictank = bsi.toilettypeftconnseptictank,
    --   toilettypeftconnpit = bsi.toilettypeftconnpit,
    --   toilettypeftconnanotherloc = bsi.toilettypeftconnanotherloc,
    --   toilettypevippitlatrine = bsi.toilettypevippitlatrine,
    --   toilettypepitlatrinewithfloor = bsi.toilettypepitlatrinewithfloor,
    --   toilettypeopenpitlatrine = bsi.toilettypeopenpitlatrine,
    --   toilettypeothers = bsi.toilettypeothers,
    --   burningchambers = bsi.burningchambers,
    --   septictanks = bsi.septictanks,
    --   watertanks = bsi.watertanks,
    --   specialgirlsroom = bsi.specialgirlsroom


--------------------------------------------------------------------------
-- IMPROVED TOILET TYPES 
-- Check when defining improved toilet types any observations where all categories of toilet type are NULL 
-- Output count and the school registration numbers in those cases. Adapt the code below to do that 
 UPDATE visualization.bemis_school_comb_vis 
      SET improved_toilet_type = TRUE 
      WHERE toilettypeftconnmainsewerline = TRUE
         OR toilettypeftconnseptictank = TRUE 
         OR toilettypeftconnpit = TRUE  
         OR toilettypevippitlatrine = TRUE

-- check output stats around imporved toilet types and output those 
SELECT 
    improved_toilet_type, 
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM visualization.bemis_school_comb_vis
GROUP BY bemis_school_comb_vis.improved_toilet_type
ORDER BY count DESC;

--------------------------------------------------------------------------
-- IMPROVED WATER SOURCES 
-- check distribution on improved water source variable once generated and output the summary stats
SELECT 
    improved_water_source, 
    COUNT(*) AS count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS percentage
FROM visualization.bemis_school_comb_vis
GROUP BY improved_water_source
ORDER BY count DESC;

-- there are some nulls but they correspond to where there is infrastructure data and no service data - 582 schools.

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
--------------------------------------------------------------------------



--------------------------------------------------------------------------

END;
$$;