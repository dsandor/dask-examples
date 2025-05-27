-- Drop the existing function if it exists
DROP FUNCTION IF EXISTS public.merge_jsonb_from_temp(TEXT, TEXT, TEXT, TEXT, TEXT[]);

-- Create a simplified version of the function
CREATE OR REPLACE FUNCTION public.merge_jsonb_from_temp(
    p_temp_table TEXT,
    p_id_column TEXT,
    p_target_table TEXT,
    p_target_jsonb_column TEXT,
    p_exclude_columns TEXT[] DEFAULT '{}'::TEXT[]
) RETURNS VOID AS $func$
DECLARE
    v_sql TEXT;
    v_source_columns TEXT;
    v_exclude_columns TEXT[];
    v_source_id_column TEXT;
    v_target_id_column TEXT;
    v_columns_to_include TEXT;
    v_column_record RECORD;
    v_temp_sql TEXT;
    v_result RECORD;
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
    
    -- Get all columns from the source table
    v_columns_to_include := '';
    FOR v_column_record IN 
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = p_temp_table 
        AND column_name <> ALL(v_exclude_columns)
    LOOP
        IF v_columns_to_include <> '' THEN
            v_columns_to_include := v_columns_to_include || ', ';
        END IF;
        v_columns_to_include := v_columns_to_include || format('%I', v_column_record.column_name);
    END LOOP;
    
    -- Build the dynamic SQL to create a temporary table with the transformed data
    v_temp_sql := format('CREATE TEMP TABLE temp_merged_data AS 
        SELECT 
            %I as id_value,
            jsonb_object_agg(
                lower(key),
                CASE 
                    WHEN value = '''' THEN NULL
                    WHEN value ~ ''^[0-9]+(\\.?[0-9]+)?$'' THEN to_jsonb(value::numeric)
                    WHEN lower(value) = ''true'' THEN to_jsonb(true)
                    WHEN lower(value) = ''false'' THEN to_jsonb(false)
                    WHEN value ~ ''^\\d{4}-\\d{2}-\\d{2}(T| )?\\d{2}:\\d{2}:\\d{2}'' THEN to_jsonb(value::timestamp)
                    WHEN value ~ ''^\\d{4}-\\d{2}-\\d{2}$'' THEN to_jsonb(value::date)
                    ELSE to_jsonb(value)
                END
            ) as json_data
        FROM (
            SELECT %I, (%s)::jsonb as data
            FROM %I
        ) t, 
        jsonb_each_text(t.data) 
        GROUP BY %I',
        v_source_id_column,
        v_source_id_column,
        'SELECT ' || v_columns_to_include || ' FROM ' || p_temp_table || ' t2 WHERE t2."' || v_source_id_column || '" = t."' || v_source_id_column || '"',
        p_temp_table,
        v_source_id_column
    );
    
    -- Execute the dynamic SQL to create the temporary table
    EXECUTE v_temp_sql;
    
    -- Now perform the update using the temporary table
    v_sql := format('UPDATE %I t SET %I = COALESCE(t.%I, ''{}''::jsonb) || s.json_data FROM temp_merged_data s WHERE t.%I::text = s.id_value::text',
        p_target_table,
        p_target_jsonb_column,
        p_target_jsonb_column,
        v_target_id_column
    );
    
    -- For debugging
    RAISE NOTICE 'Executing update SQL: %', v_sql;
    
    -- Execute the update
    EXECUTE v_sql;
    
    -- Clean up
    DROP TABLE IF EXISTS temp_merged_data;
    
    RAISE NOTICE 'Successfully merged data from %.% to %.%', 
        p_temp_table, v_source_id_column, 
        p_target_table, p_target_jsonb_column;
    
EXCEPTION WHEN OTHERS THEN
    -- Clean up in case of error
    DROP TABLE IF EXISTS temp_merged_data;
    RAISE EXCEPTION 'Error in merge_jsonb_from_temp: % (SQL: %s)', SQLERRM, COALESCE(v_sql, 'N/A');
END;
$func$ LANGUAGE plpgsql;
