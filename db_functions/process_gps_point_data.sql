
CREATE OR REPLACE PROCEDURE process_wp_gps_data()
LANGUAGE plpgsql
AS $$
BEGIN
     --------------------------------------------------------------------------
    -- Step 1: Drop  water_point_report_with_locations Table if It Exists
    --------------------------------------------------------------------------
    DROP TABLE IF EXISTS visualization.water_point_report_with_locations;

    --------------------------------------------------------------------------
    -- Step 2: Create the water_point_report_with_locations table
    --------------------------------------------------------------------------
    CREATE TABLE visualization.water_point_report_with_locations AS
        SELECT DISTINCT ON (wp.dpid) 
            wp.dpid,
            (SELECT region_name FROM visualization.region_district_lga_names rdl 
            WHERE wp.region = rdl.region_code LIMIT 1) AS region_name,
            (SELECT district_name FROM visualization.region_district_lga_names rdl 
            WHERE wp.district_code = rdl.district_code LIMIT 1) AS district_name,
            (SELECT lga_name FROM visualization.region_district_lga_names rdl 
            WHERE wp.lga = rdl.lga_code LIMIT 1) AS lga_name,
            (SELECT villagename FROM foreign_schema_ruwasa_rsdms.ruwasa_villages v  
            WHERE wp.village = v.villagecode LIMIT 1) AS village_name,
            wp.ward AS ward_name,
            wp.functionalitystatus,
            wp.populationserved,
            wp.latitude,
            wp.longitude,
            wp.report_datetime,
            (SELECT geojson FROM visualization.region_district_lga_names rdl 
            WHERE wp.lga = rdl.lga_code LIMIT 1) AS geojson
        FROM visualization.ruwasa_wp_report_vis wp
        WHERE wp.latitude IS NOT NULL
        AND wp.longitude IS NOT NULL
        AND wp.functionalitystatus IS NOT NULL 
        AND wp.report_datetime >= date_trunc('month', current_date - interval '3 month')
        AND wp.report_datetime < date_trunc('month', current_date)
        AND wp.functionalitystatus IN('functional', 'not_functional', 'functional_need_repair') 
        ORDER BY wp.dpid, wp.report_datetime DESC;
        
END;
$$;

-- call process

CALL process_wp_gps_data();
