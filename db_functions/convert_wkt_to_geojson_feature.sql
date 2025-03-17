-- ============================================================================
-- Stored Procedure: convert_wkt_to_geojson_feature
-- 
-- Purpose: Converts WKT (Well-Known Text) geometries in a database table to 
-- GeoJSON Feature format, preserving all other columns as properties.
--
-- This is particularly useful for charts that require
-- GeoJSON formatted data for visualization.
-- ============================================================================
CREATE OR REPLACE PROCEDURE convert_wkt_to_geojson_feature(
  p_table_name     text,  -- e.g. 'public.district_boundaries' or 'district_boundaries'
  p_wkt_column     text,  -- column name that contains the WKT, e.g. 'wkt'
  p_geojson_column text DEFAULT 'geojson',  -- column to store the GeoJSON output
  p_srid           integer DEFAULT 4326     -- Spatial Reference ID (4326 = WGS84, standard for web maps)
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_schema text;  -- Will hold the schema part of the table name
  v_table  text;  -- Will hold the table part of the table name
  v_sql    text;  -- Used to build dynamic SQL statements
BEGIN
  -- Split p_table_name into schema and table parts if a dot is present.
  -- This allows flexibility in specifying tables with or without schema.
  IF position('.' in p_table_name) > 0 THEN
    v_schema := split_part(p_table_name, '.', 1);
    v_table  := split_part(p_table_name, '.', 2);
  ELSE
    -- If no schema specified, use the current schema context
    v_schema := current_schema;
    v_table  := p_table_name;
  END IF;

  -- Validation step: Check if the target table exists.
  -- This prevents errors when trying to modify non-existent tables.
  PERFORM 1
  FROM information_schema.tables
  WHERE table_schema = v_schema
    AND table_name = v_table;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Table %.% does not exist', v_schema, v_table;
  END IF;

  -- Validation step: Check if the provided WKT column exists.
  -- This ensures we're not trying to read from a non-existent column.
  PERFORM 1
  FROM information_schema.columns
  WHERE table_schema = v_schema
    AND table_name = v_table
    AND column_name = p_wkt_column;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Column "%" does not exist in table %.%', p_wkt_column, v_schema, v_table;
  END IF;

  -- Prepare the destination column: Create the GeoJSON column if it doesn't exist.
  -- Using 'ADD COLUMN IF NOT EXISTS' makes the procedure idempotent (safe to run multiple times).
  v_sql := format(
    'ALTER TABLE %I.%I ADD COLUMN IF NOT EXISTS %I text;',
    v_schema, v_table, p_geojson_column
  );
  EXECUTE v_sql;

  -- Core conversion step: Transform WKT to GeoJSON geometry
  -- This involves several spatial operations:
  --   1. ST_GeomFromText: Converts WKT string to PostGIS geometry type
  --   2. ST_ForceRHR: Ensures polygon orientation follows the right-hand rule
  --   3. ST_Reverse: Additionally ensures correct orientation for proper rendering
  --   4. ST_AsGeoJSON: Converts PostGIS geometry to GeoJSON format
  v_sql := format(
    'UPDATE %I.%I 
       SET %I = public.ST_AsGeoJSON(
                     public.ST_Reverse(
                       public.ST_ForceRHR(
                         public.ST_GeomFromText(%I, %s)
                       )
                     )
                   )::text;',
    v_schema, v_table, p_geojson_column, p_wkt_column, p_srid
  );
  EXECUTE v_sql;


  -- Final step: Enhance the GeoJSON structure to a complete Feature object
  -- A GeoJSON Feature combines geometry with properties (attributes)
  -- This step takes all other columns in the row and includes them as properties,
  -- creating a fully-formed GeoJSON Feature object per the GeoJSON specification.
  v_sql := format(
    'UPDATE %I.%I t SET %I = json_build_object(
       ''type'', ''Feature'',
       ''properties'', to_jsonb(t) - %L - %L,  -- Convert row to JSON, excluding geometry columns
       ''geometry'', t.%I::json                -- Include the geometry we created earlier
     )::text;',
    v_schema, v_table, p_geojson_column, p_wkt_column, p_geojson_column, p_geojson_column
  );
  EXECUTE v_sql;

  -- Provide feedback about successful completion
  RAISE NOTICE 'Updated table %.%: WKT converted to GeoJSON Feature in column %I.', v_schema, v_table, p_geojson_column;
END;
$$;
