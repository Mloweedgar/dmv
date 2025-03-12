
--  Create a Procedure to Populate the Cross-Cutting Wash Data Visualization Table
-- Note: this script should be run after running other scripts for processing nsmis,bemis and rsdms data. 
-- so that the tables needed for cross cutting data processing will be ready
-- Recommandation: this script can be scheduled to run monthly
CREATE OR REPLACE PROCEDURE process_cross_cutting_wash_data()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1: Drop Existing Visualization Table if It Exists
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.cross_cutting_wash_data_vis';
    
    --------------------------------------------------------------------------
    -- Step 2: Create the Visualization Table with Joined NSMIS, RUWASA LGA, and Village Data
    --------------------------------------------------------------------------
    CREATE TABLE visualization.cross_cutting_wash_data_vis AS  
    SELECT 
        l.lgacode,  
        l.nsmislgacode,
        l.bemislgacode,
        l.lganame,
        l.districtcode,
        l.geojson,
        (SELECT rd.districtname FROM public.ruwasa_districts rd where rd.districtcode = l.districtcode) district_name,
        CASE 
            WHEN ROUND(AVG(v.infracoverage::INTEGER), 1) = 0 THEN NULL 
            ELSE ROUND(AVG(v.infracoverage::INTEGER), 1) 
        END AS lga_water_access_level_perc,
        ROUND(AVG(n.improved_perc_hhs::NUMERIC)*100, 1) AS lga_improved_san_perc_hhs, 
        ROUND(AVG(n.handwashstation_perc_hhs::NUMERIC)*100, 1) AS lga_handwashstation_perc_hhs,
        ROUND(AVG(n.handwashsoap_perc_hhs::NUMERIC)*100,1) AS lga_handwashsoap_perc_hhs,
        FIRST_VALUE(n.regioncode) OVER (PARTITION BY l.lgacode) AS regioncode,  
        FIRST_VALUE(n.region_name) OVER (PARTITION BY l.lgacode) AS region_name  
    FROM visualization.ruwasa_lgas_with_geojson l  
    INNER JOIN visualization.nsmis_household_sanitation_reports_vis n  
        ON n.lgacode = l.nsmislgacode
    INNER JOIN foreign_schema_ruwasa_rsdms.ruwasa_villages v  
        ON v.lgacode = l.lgacode
    GROUP BY 
        l.lgacode,  
        l.nsmislgacode,
        l.bemislgacode,
        l.lganame,
        l.geojson,
        l.districtcode,
        n.regioncode,
        n.region_name;
    
     --------------------------------------------------------------------------
    -- Step 3: Drop  water_point_report_with_locations Table if It Exists
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
