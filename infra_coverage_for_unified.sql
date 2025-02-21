-- Creating the table with average 'infracoverage' per district
DROP TABLE IF EXISTS visualization.ruwasa_district_infracoverage;

CREATE TABLE visualization.ruwasa_district_infracoverage AS
SELECT district, AVG(infracoverage) AS avg_infracoverage
FROM foreign_schema_ruwasa_rsdms.ruwasa_reports_coverage
WHERE report_datetime::DATE = (
    SELECT MAX(report_datetime::DATE)
    FROM foreign_schema_ruwasa_rsdms.ruwasa_reports_coverage
) AND infracoverage > 0 
GROUP BY district;

-- Adding columns to the new table
ALTER TABLE visualization.ruwasa_district_infracoverage
  ADD COLUMN region_code VARCHAR,
  ADD COLUMN region_name VARCHAR,
  ADD COLUMN district_name VARCHAR(255);

-- Updating the 'district_name' in the 'ruwasa_district_infracoverage' table
UPDATE visualization.ruwasa_district_infracoverage
  SET 
    district_name = n.district_name,
    region_name = n.region_name,
    region_code = n.region_code
FROM visualization.region_district_lga_names AS n
WHERE visualization.ruwasa_district_infracoverage.district = n.district_code;

-- Renaming column 'district' to 'district_code' in the 'ruwasa_wp_report_vis' table
ALTER TABLE visualization.ruwasa_district_infracoverage
RENAME COLUMN district TO district_code;

-- Correcting the final SELECT query
SELECT * FROM visualization.ruwasa_district_infracoverage LIMIT 10;