-- Function to merge data from a temporary table into a target table with JSONB data
-- This function performs a set-based operation to efficiently merge the data

-- First, let's create a function that will handle the merge logic
CREATE OR REPLACE FUNCTION merge_jsonb_from_temp(
    p_temp_table TEXT,
    p_id_column TEXT,
    p_target_table TEXT,
    p_target_jsonb_column TEXT,
    p_exclude_columns TEXT[] DEFAULT '{}'::TEXT[]
) RETURNS VOID AS $func$
DECLARE
    v_sql TEXT;
    v_columns TEXT;
    v_update_columns TEXT;
    v_column_record RECORD;
    v_exclude_columns TEXT[];
BEGIN
    -- Set default excluded columns if not provided
    v_exclude_columns := COALESCE(p_exclude_columns, '{}'::TEXT[]);
    
    -- Add id_column to excluded columns to prevent including it in the JSONB
    v_exclude_columns := array_append(v_exclude_columns, p_id_column);
    
    -- Build the dynamic SQL to generate the JSONB object from the temp table
    -- and merge it with existing JSONB data
    v_sql := format($sql$
        WITH source_data AS (
            SELECT 
                %I,
                (
                    SELECT jsonb_object_agg(
                        lower(key),
                        CASE 
                            WHEN value = '' THEN NULL
                            WHEN value ~ ''^[0-9]+(\.?[0-9]+)?$'' THEN to_jsonb(value::numeric)
                            WHEN value ~ ''^(true|false)$'' THEN to_jsonb(value::boolean)
                            WHEN value ~ ''^[0-9]{4}-[0-9]{2}-[0-9]{2}'' THEN to_jsonb(value::date)
                            ELSE to_jsonb(value)
                        END
                    )
                    FROM jsonb_each_text(
                        (SELECT to_jsonb(t) - %L FROM (SELECT * FROM %I WHERE %I = src.%I) t)
                    )
                ) as new_data
            FROM %I src
        )
        UPDATE %I t
        SET %I = (
            SELECT 
                jsonb_strip_nulls(
                    jsonb_object_agg(
                        key, 
                        COALESCE(
                            new_data->key, 
                            t.%I->key
                        )
                    )
                )
            FROM (
                SELECT key, value 
                FROM jsonb_each(COALESCE(new_data, ''{}''::jsonb) || COALESCE(t.%I, ''{}''::jsonb))
            ) s
        )
        FROM source_data sd
        WHERE t.%I = sd.%I
        AND sd.new_data IS NOT NULL;
    '', 
    p_id_column,  -- %I (1)
    v_exclude_columns,  -- %L (2)
    p_temp_table,  -- %I (3)
    p_id_column,  -- %I (4)
    p_id_column,  -- %I (5)
    p_temp_table,  -- %I (6)
    p_target_table,  -- %I (7)
    p_target_jsonb_column,  -- %I (8)
    p_target_jsonb_column,  -- %I (9)
    p_target_jsonb_column,  -- %I (10)
    p_id_column,  -- %I (11)
    p_id_column  -- %I (12)
    );
    
    -- Execute the dynamic SQL
    EXECUTE v_sql;
    
    -- Log the merge operation
    RAISE NOTICE 'Merged data from % to %.%', p_temp_table, p_target_table, p_target_jsonb_column;
    
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Error in merge_jsonb_from_temp: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
/*
-- 1. Create a temporary table with your data
CREATE TEMP TABLE temp_import_data (
    id_bb_global TEXT PRIMARY KEY,
    name TEXT,
    ticker TEXT,
    currency TEXT,
    -- other columns...
);

-- 2. Load your data into the temp table (using COPY or INSERT)
-- COPY temp_import_data FROM '/path/to/your/file.csv' WITH (FORMAT csv, HEADER true);

-- 3. Call the merge function
SELECT merge_jsonb_from_temp(
    'temp_import_data',  -- temp table name
    'id_bb_global',     -- ID column name
    'target_table',      -- target table name
    'jsonb_column',     -- JSONB column in target table
    ARRAY['created_at', 'updated_at']  -- columns to exclude from merge
);
*/

-- Function to get the merge SQL without executing it (for debugging)
CREATE OR REPLACE FUNCTION get_merge_jsonb_sql(
    p_temp_table TEXT,
    p_id_column TEXT,
    p_target_table TEXT,
    p_target_jsonb_column TEXT,
    p_exclude_columns TEXT[] DEFAULT '{}'::TEXT[]
) RETURNS TEXT AS $$
DECLARE
    v_sql TEXT;
    v_exclude_columns TEXT[];
BEGIN
    v_exclude_columns := COALESCE(p_exclude_columns, '{}'::TEXT[]);
    v_exclude_columns := array_append(v_exclude_columns, p_id_column);
    
    v_sql := format($f$
        -- This is the SQL that would be executed by merge_jsonb_from_temp:
        WITH source_data AS (
            SELECT 
                %I,
                (
                    SELECT jsonb_object_agg(
                        lower(key),
                        CASE 
                            WHEN value = '' THEN NULL
                            WHEN value ~ '^[0-9]+(\\.?[0-9]+)?$' THEN to_jsonb(value::numeric)
                            WHEN value ~ '^(true|false)$' THEN to_jsonb(value::boolean)
                            WHEN value ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}' THEN to_jsonb(value::date)
                            ELSE to_jsonb(value)
                        END
                    )
                    FROM jsonb_each_text(
                        (SELECT to_jsonb(t) - %L FROM (SELECT * FROM %I WHERE %I = src.%I) t)
                    )
                ) as new_data
            FROM %I src
        )
        UPDATE %I t
        SET %I = (
            SELECT 
                jsonb_strip_nulls(
                    jsonb_object_agg(
                        key, 
                        COALESCE(
                            new_data->key, 
                            t.%I->key
                        )
                    )
                )
            FROM (
                SELECT key, value 
                FROM jsonb_each(COALESCE(new_data, '{}'::jsonb) || COALESCE(t.%I, '{}'::jsonb))
            ) s
        )
        FROM source_data sd
        WHERE t.%I = sd.%I
        AND sd.new_data IS NOT NULL
    $f$
    '', 
    p_id_column, v_exclude_columns, p_temp_table, p_id_column, p_id_column, 
    p_temp_table, p_target_table, p_target_jsonb_column, p_target_jsonb_column, 
    p_target_jsonb_column, p_id_column, p_id_column);
    
    RETURN v_sql;
END;
$$ LANGUAGE plpgsql;
