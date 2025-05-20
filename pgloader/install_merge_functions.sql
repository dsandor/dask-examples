-- Drop existing functions if they exist
DROP FUNCTION IF EXISTS merge_jsonb_from_temp(TEXT, TEXT, TEXT, TEXT, TEXT[]);
DROP FUNCTION IF EXISTS get_merge_jsonb_sql(TEXT, TEXT, TEXT, TEXT, TEXT[]);

-- Function to merge data from a temporary table into a target table with JSONB data
CREATE OR REPLACE FUNCTION merge_jsonb_from_temp(
    p_temp_table TEXT,
    p_id_column TEXT,
    p_target_table TEXT,
    p_target_jsonb_column TEXT,
    p_exclude_columns TEXT[] DEFAULT '{}'::TEXT[]
) RETURNS VOID AS $func$
DECLARE
    v_sql TEXT;
    v_exclude_columns TEXT[];
    v_reserved_keywords TEXT[] := ARRAY[
        'rownumber', 'order', 'group', 'user', 'table', 'column',
        'select', 'from', 'where', 'update', 'delete', 'insert',
        'create', 'drop', 'alter', 'index', 'view', 'sequence',
        'trigger', 'function', 'procedure', 'schema', 'database',
        'constraint', 'primary', 'foreign', 'key', 'unique',
        'check', 'default', 'null', 'not', 'and', 'or', 'as',
        'on', 'in', 'exists', 'between', 'like', 'ilike', 'is',
        'all', 'any', 'some', 'distinct', 'having', 'limit',
        'offset', 'union', 'intersect', 'except', 'case', 'when',
        'then', 'else', 'end', 'true', 'false', 'unknown'
    ];
BEGIN
    -- Set default excluded columns if not provided
    v_exclude_columns := COALESCE(p_exclude_columns, '{}'::TEXT[]);
    v_exclude_columns := array_append(v_exclude_columns, p_id_column);
    
    -- Build the dynamic SQL
    v_sql := format($sql$
        WITH source_data AS (
            SELECT 
                %1$I,
                (
                    SELECT jsonb_object_agg(
                        CASE 
                            WHEN key = ANY(%2$L) THEN '_' || key
                            ELSE lower(key)
                        END,
                        CASE 
                            WHEN value = '' THEN NULL
                            WHEN value ~ '^[0-9]+(\\.?[0-9]+)?$' THEN to_jsonb(value::numeric)
                            WHEN lower(value) IN ('true', 'false') THEN to_jsonb(lower(value)::boolean)
                            WHEN value ~ '^\\d{4}-\\d{2}-\\d{2}(T| )?\\d{2}:\\d{2}:\\d{2}' THEN to_jsonb(value::timestamp)
                            WHEN value ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN to_jsonb(value::date)
                            ELSE to_jsonb(value)
                        END
                    )
                    FROM jsonb_each_text(
                        (SELECT to_jsonb(t) - %3$L::text[] FROM (SELECT * FROM %4$I LIMIT 1) t)
                    )
                    WHERE key <> ALL(%3$L)
                ) as new_data
            FROM %4$I
        )
        UPDATE %5$I t
        SET %6$I = COALESCE(t.%6$I, '{}'::jsonb) || sd.new_data
        FROM source_data sd
        WHERE t.%1$I = sd.%1$I
        AND sd.new_data IS NOT NULL;
    $sql$,
    p_id_column, v_reserved_keywords, v_exclude_columns, p_temp_table, p_target_table, p_target_jsonb_column
    );
    
    -- Execute the dynamic SQL
    EXECUTE v_sql;
    
    -- Log the merge operation
    RAISE NOTICE 'Merged data from % to %.%', p_temp_table, p_target_table, p_target_jsonb_column;
    
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Error in merge_jsonb_from_temp: %', SQLERRM;
END;
$func$ LANGUAGE plpgsql;

-- Function to get the merge SQL without executing it (for debugging)
CREATE OR REPLACE FUNCTION get_merge_jsonb_sql(
    p_temp_table TEXT,
    p_id_column TEXT,
    p_target_table TEXT,
    p_target_jsonb_column TEXT,
    p_exclude_columns TEXT[] DEFAULT '{}'::TEXT[]
) RETURNS TEXT AS $func$
DECLARE
    v_sql TEXT;
    v_exclude_columns TEXT[];
    v_reserved_keywords TEXT[] := ARRAY[
        'rownumber', 'order', 'group', 'user', 'table', 'column',
        'select', 'from', 'where', 'update', 'delete', 'insert',
        'create', 'drop', 'alter', 'index', 'view', 'sequence',
        'trigger', 'function', 'procedure', 'schema', 'database',
        'constraint', 'primary', 'foreign', 'key', 'unique',
        'check', 'default', 'null', 'not', 'and', 'or', 'as',
        'on', 'in', 'exists', 'between', 'like', 'ilike', 'is',
        'all', 'any', 'some', 'distinct', 'having', 'limit',
        'offset', 'union', 'intersect', 'except', 'case', 'when',
        'then', 'else', 'end', 'true', 'false', 'unknown'
    ];
BEGIN
    v_exclude_columns := COALESCE(p_exclude_columns, '{}'::TEXT[]);
    v_exclude_columns := array_append(v_exclude_columns, p_id_column);
    
    v_sql := format($sql$
        -- This is the SQL that would be executed by merge_jsonb_from_temp:
        WITH source_data AS (
            SELECT 
                %1$I,
                (
                    SELECT jsonb_object_agg(
                        CASE 
                            WHEN key = ANY(%2$L) THEN '_' || key
                            ELSE lower(key)
                        END,
                        CASE 
                            WHEN value = '' THEN NULL
                            WHEN value ~ '^[0-9]+(\\\\.?[0-9]+)?$' THEN to_jsonb(value::numeric)
                            WHEN lower(value) IN ('true', 'false') THEN to_jsonb(lower(value)::boolean)
                            WHEN value ~ '^\\\\d{4}-\\\\d{2}-\\\\d{2}(T| )?\\\\d{2}:\\\\d{2}:\\\\d{2}' THEN to_jsonb(value::timestamp)
                            WHEN value ~ '^\\\\d{4}-\\\\d{2}-\\\\d{2}$' THEN to_jsonb(value::date)
                            ELSE to_jsonb(value)
                        END
                    )
                    FROM jsonb_each_text(
                        (SELECT to_jsonb(t) - %3$L::text[] FROM (SELECT * FROM %4$I LIMIT 1) t)
                    )
                    WHERE key <> ALL(%3$L)
                ) as new_data
            FROM %4$I
        )
        UPDATE %5$I t
        SET %6$I = COALESCE(t.%6$I, '{}'::jsonb) || sd.new_data
        FROM source_data sd
        WHERE t.%1$I = sd.%1$I
        AND sd.new_data IS NOT NULL;
    $sql$,
    p_id_column, v_reserved_keywords, v_exclude_columns, p_temp_table, p_target_table, p_target_jsonb_column
    );
    
    RETURN v_sql;
END;
$func$ LANGUAGE plpgsql;

-- Example usage:
/*
-- 1. Create a temporary table with your data
CREATE TEMP TABLE temp_import_data (
    id_bb_global TEXT PRIMARY KEY,
    name TEXT,
    ticker TEXT,
    currency TEXT
);

-- 2. Load your data into the temp table
-- \copy temp_import_data FROM 'data.csv' WITH (FORMAT csv, HEADER true);

-- 3. Call the merge function
SELECT merge_jsonb_from_temp(
    'temp_import_data',  -- temp table name
    'id_bb_global',     -- ID column name
    'target_table',     -- target table name
    'data',             -- JSONB column in target table
    ARRAY['created_at', 'updated_at']  -- columns to exclude from merge
);

-- 4. To see the SQL that would be executed:
SELECT get_merge_jsonb_sql(
    'temp_import_data',
    'id_bb_global',
    'target_table',
    'data',
    ARRAY['created_at', 'updated_at']
);
*/
