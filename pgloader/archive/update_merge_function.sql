-- Drop the existing function if it exists
DROP FUNCTION IF EXISTS public.merge_jsonb_from_temp(TEXT, TEXT, TEXT, TEXT, TEXT[]);

-- Create a more robust version of the function
CREATE OR REPLACE FUNCTION public.merge_jsonb_from_temp(
    p_temp_table TEXT,
    p_id_column TEXT,
    p_target_table TEXT,
    p_target_jsonb_column TEXT,
    p_exclude_columns TEXT[] DEFAULT '{}'::TEXT[]
) RETURNS VOID AS $func$
DECLARE
    v_sql TEXT;
    v_exclude_columns TEXT[];
    v_source_id_column TEXT;
    v_target_id_column TEXT;
    v_columns TEXT;
    v_column_record RECORD;
BEGIN
    -- Set default excluded columns if not provided
    v_exclude_columns := COALESCE(p_exclude_columns, '{}'::TEXT[]);
    v_exclude_columns := array_append(v_exclude_columns, p_id_column);
    
    -- Get the actual case-sensitive column name from source table
    EXECUTE format('SELECT column_name FROM information_schema.columns ' ||
                  'WHERE table_name = %L AND lower(column_name) = lower(%L) ' ||
                  'LIMIT 1', 
                  p_temp_table, p_id_column) 
    INTO v_source_id_column;
    
    IF v_source_id_column IS NULL THEN
        RAISE EXCEPTION 'Column % does not exist in source table %', p_id_column, p_temp_table;
    END IF;
    
    -- Get the actual case-sensitive column name from target table
    EXECUTE format('SELECT column_name FROM information_schema.columns ' ||
                  'WHERE table_name = %L AND lower(column_name) = lower(%L) ' ||
                  'LIMIT 1', 
                  p_target_table, p_id_column) 
    INTO v_target_id_column;
    
    IF v_target_id_column IS NULL THEN
        RAISE EXCEPTION 'Column % does not exist in target table %', p_id_column, p_target_table;
    END IF;
    
    -- Build the dynamic SQL
    v_sql := format($sql$
        WITH source_data AS (
            SELECT 
                %1$I,
                (
                    SELECT jsonb_object_agg(
                        lower(key),
                        CASE 
                            WHEN value = '''' THEN NULL
                            WHEN value ~ '^[0-9]+(\\.?[0-9]+)?$' THEN to_jsonb(value::numeric)
                            WHEN lower(value) = 'true' THEN to_jsonb(true)
                            WHEN lower(value) = 'false' THEN to_jsonb(false)
                            WHEN value ~ '^\\\\d{4}-\\\\d{2}-\\\\d{2}(T| )?\\\\d{2}:\\\\d{2}:\\\\d{2}' THEN to_jsonb(value::timestamp)
                            WHEN value ~ '^\\\\d{4}-\\\\d{2}-\\\\d{2}$' THEN to_jsonb(value::date)
                            ELSE to_jsonb(value)
                        END
                    )
                    FROM jsonb_each_text(
                        (SELECT to_jsonb(t) - %2$L::text[] FROM %3$I t LIMIT 1)::jsonb
                    )
                    WHERE key <> ALL(%2$L)
                ) as new_data
            FROM %3$I
        )
        UPDATE %4$I t
        SET %5$I = COALESCE(t.%5$I, '{}'::jsonb) || sd.new_data
        FROM source_data sd
        WHERE t.%6$I = sd.%1$I
        AND sd.new_data IS NOT NULL;
    $sql$,
    v_source_id_column,  -- %1$I - Source ID column (case-sensitive)
    v_exclude_columns,   -- %2$L - Excluded columns
    p_temp_table,        -- %3$I - Source table
    p_target_table,      -- %4$I - Target table
    p_target_jsonb_column, -- %5$I - Target JSONB column
    v_target_id_column   -- %6$I - Target ID column (case-sensitive)
    );
    
    -- For debugging
    RAISE NOTICE 'Executing SQL: %', v_sql;
    
    -- Execute the dynamic SQL
    EXECUTE v_sql;
    
    RAISE NOTICE 'Successfully merged data from %.% to %.%', 
        p_temp_table, v_actual_id_column, 
        p_target_table, p_target_jsonb_column;
    
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Error in merge_jsonb_from_temp: % (SQL: %s)', SQLERRM, v_sql;
END;
$func$ LANGUAGE plpgsql;

-- Test the function
-- SELECT public.merge_jsonb_from_temp(
--     'temp_equity_namr_obfu_shkl',
--     'ID_BB_GLOBAL',
--     'csv_data',
--     'data',
--     ARRAY['created_at', 'updated_at']
-- );
