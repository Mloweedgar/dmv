-- Join NSMIS data with LGA code from ruwasa, and RUWASA village data for population water coverage  

DROP TABLE IF EXISTS visualization.cross_cutting_wash_data_vis;
  
CREATE TABLE visualization.cross_cutting_wash_data_vis AS  
SELECT 
    l.lgacode,  
    l.nsmislgacode,
    l.bemislgacode,
    l.lganame,
    l.districtcode,
    CASE 
        WHEN ROUND(AVG(v.infracoverage::INTEGER), 1) = 0 THEN NULL 
        ELSE ROUND(AVG(v.infracoverage::INTEGER), 1) 
    END AS lga_water_access_level_perc,
    ROUND(AVG(n.improved_perc_hhs::NUMERIC)*100, 1) AS lga_improved_san_perc_hhs, 
    ROUND(AVG(n.handwashstation_perc_hhs::NUMERIC)*100, 1) AS lga_handwashstation_perc_hhs,
    ROUND(AVG(n.handwashsoap_perc_hhs::NUMERIC)*100,1) AS lga_handwashsoap_perc_hhs,
    FIRST_VALUE(n.regioncode) OVER (PARTITION BY l.lgacode) AS regioncode,  
    FIRST_VALUE(n.region_name) OVER (PARTITION BY l.lgacode) AS region_name  
FROM public.ruwasa_lgas l  
INNER JOIN visualization.nsmis_household_sanitation_reports_vis n  
    ON n.lgacode = l.nsmislgacode
INNER JOIN foreign_schema_ruwasa_rsdms.ruwasa_villages v  
    ON v.lgacode = l.lgacode
GROUP BY 
    l.lgacode,  
    l.nsmislgacode,
    l.bemislgacode,
    l.lganame,
    l.districtcode,
    n.regioncode,
    n.region_name;

SELECT * FROM visualization.cross_cutting_wash_data_vis LIMIT 200;

