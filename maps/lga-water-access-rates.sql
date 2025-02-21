-- data source: visualization.cross_cutting_wash_data_vis
-- 1. modify visualization.cross_cutting_wash_data_vis add column geojson
-- 2. grab geojson data from visualization.ruwasa_lgas_with_geojson. use lgacode for the join
-- 3. update visualization.cross_cutting_wash_data_vis with the geojson data

-- 1. Add geojson column to cross_cutting_wash_data_vis
ALTER TABLE visualization.cross_cutting_wash_data_vis
ADD COLUMN geojson VARCHAR;

-- 2. Update the geojson data from ruwasa_lgas_with_geojson
UPDATE visualization.cross_cutting_wash_data_vis cw
SET geojson = r.geojson
FROM visualization.ruwasa_lgas_with_geojson r
WHERE cw.lgacode = r.lgacode;

-- Verify the update
SELECT 
    lgacode,
    geojson IS NOT NULL as has_geojson
FROM visualization.cross_cutting_wash_data_vis
WHERE geojson IS NULL;



-- show status in diffent colors --> water point status map
DROP TABLE IF EXISTS visualization.water_point_report_with_locations;

CREATE TABLE visualization.water_point_report_with_locations AS
SELECT DISTINCT ON (wp.dpid) 
    wp.dpid,
    (SELECT region_name FROM visualization.region_district_lga_names rdl 
     WHERE wp.region = rdl.region_code LIMIT 1) AS region_name,
    (SELECT district_name FROM visualization.region_district_lga_names rdl 
     WHERE wp.district = rdl.district_code LIMIT 1) AS district_name,
    (SELECT lga_name FROM visualization.region_district_lga_names rdl 
     WHERE wp.lga = rdl.lga_code LIMIT 1) AS lga_name,
    (SELECT villagename FROM foreign_schema_ruwasa_rsdms.ruwasa_villages v  
     WHERE wp.village = v.villagecode LIMIT 1) AS village_name,
    wp.ward AS ward_name,
    wp.functionalitystatus,
    wp.populationserved,
    wp.latitude,
    wp.longitude,
    wp.report_datetime
FROM visualization.ruwasa_wp_report_vis wp
WHERE wp.latitude IS NOT NULL
  AND wp.longitude IS NOT NULL
  AND wp.functionalitystatus IS NOT NULL 
  AND wp.report_datetime >= date_trunc('month', current_date - interval '3 month')
  AND wp.report_datetime < date_trunc('month', current_date)
  AND wp.functionalitystatus IN('functional', 'not_functional', 'functional_need_repair') 
ORDER BY wp.dpid, wp.report_datetime DESC;

