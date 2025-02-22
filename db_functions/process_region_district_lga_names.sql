--------------------------------------------------------------------------
-- Step 1: Create a Procedure to Build the Region-District-LGA Names Table
--------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE process_region_district_lga_names()
LANGUAGE plpgsql
AS $$
BEGIN
    --------------------------------------------------------------------------
    -- Step 1.1: Drop the Existing Table if It Exists
    --------------------------------------------------------------------------
    EXECUTE 'DROP TABLE IF EXISTS visualization.region_district_lga_names';

    --------------------------------------------------------------------------
    -- Step 1.2: Create the Table by Joining Regions, Districts, and LGAs
    --------------------------------------------------------------------------
    EXECUTE '
    CREATE TABLE visualization.region_district_lga_names AS 
    SELECT 
        r.name AS region_name, 
        d.districtname AS district_name, 
        l.lganame AS lga_name,
        r.code AS region_code, 
        r.bemisregioncode, 
        r.nsmisregioncode,
        d.districtcode AS district_code,
        l.lgacode AS lga_code, 
        l.bemislgacode, 
        l.nsmislgacode,
        l.geojson
    FROM visualization.ruwasa_lgas_with_geojson l 
    JOIN public.ruwasa_districts d 
        ON l.districtcode = d.districtcode
    JOIN public.ruwasa_regions r 
        ON d.regioncode = r.code
    ';

END;
$$;
