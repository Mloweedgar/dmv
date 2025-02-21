
DROP TABLE IF EXISTS visualization.region_district_lga_names;

CREATE TABLE visualization.region_district_lga_names AS 
SELECT r.name as region_name, d.districtname as district_name, l.lganame as lga_name,
       r.code as region_code, r.bemisregioncode, r.nsmisregioncode,
       d.districtcode as district_code,
       l.lgacode as lga_code, l.bemislgacode, l.nsmislgacode
FROM public.ruwasa_lgas l 
JOIN public.ruwasa_districts d ON l.districtcode = d.districtcode
JOIN public.ruwasa_regions r ON d.regioncode = r.code