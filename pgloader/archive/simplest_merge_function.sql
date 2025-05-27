-- Drop the existing function if it exists
DROP FUNCTION IF EXISTS public.merge_jsonb_from_temp(TEXT, TEXT, TEXT, TEXT, TEXT[]);

-- Create the simplest possible version of the function
CREATE OR REPLACE FUNCTION public.merge_jsonb_from_temp(
    p_temp_table TEXT,
    p_id_column TEXT,
    p_target_table TEXT,
    p_target_jsonb_column TEXT,
    p_exclude_columns TEXT[] DEFAULT '{}'::TEXT[]
) RETURNS VOID AS $func$
DECLARE
    v_sql TEXT;
    v_columns TEXT;
    v_column_record RECORD;
    v_source_id_column TEXT;
    v_target_id_column TEXT;
    v_exclude_columns TEXT[];
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
    
    -- Build a dynamic SQL to get all columns except excluded ones
    v_columns := '';
    FOR v_column_record IN 
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = p_temp_table 
        AND column_name <> ALL(v_exclude_columns)
    LOOP
        IF v_columns <> '' THEN
            v_columns := v_columns || ', ';
        END IF;
        v_columns := v_columns || format('''%s'', %I', 
                                      v_column_record.column_name, 
                                      v_column_record.column_name);
    END LOOP;
    
    -- Build the final update SQL
    v_sql := format('UPDATE %I t SET %I = COALESCE(t.%I, ''{}''::jsonb) || subq.json_data FROM (' ||
                   '    SELECT %I as id_value, jsonb_object(ARRAY[%s]) as json_data ' ||
                   '    FROM %I' ||
                   ') subq ' ||
                   'WHERE t.%I::text = subq.id_value::text',
                   p_target_table,
                   p_target_jsonb_column,
                   p_target_jsonb_column,
                   v_source_id_column,
                   v_columns,
                   p_temp_table,
                   v_target_id_column);
    
    -- For debugging
    RAISE NOTICE 'Executing SQL: %', v_sql;
    
    -- Execute the update
    EXECUTE v_sql;
    
    RAISE NOTICE 'Successfully merged data from %.% to %.%', 
        p_temp_table, v_source_id_column, 
        p_target_table, p_target_jsonb_column;
    
EXCEPTION WHEN OTHERS THEN
    RAISE EXCEPTION 'Error in merge_jsonb_from_temp: % (SQL: %s)', SQLERRM, COALESCE(v_sql, 'N/A');
END;
$func$ LANGUAGE plpgsql;
