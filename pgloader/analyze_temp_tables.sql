-- First, let's get a count of temp tables and their sizes
WITH temp_table_info AS (
    SELECT 
        table_name,
        COUNT(*) as column_count
    FROM INFORMATION_SCHEMA.columns 
    WHERE table_name LIKE 'temp%'
    GROUP BY table_name
    ORDER BY table_name
)
SELECT 
    table_name,
    column_count,
    pg_size_pretty(pg_total_relation_size(table_name)) as table_size
FROM temp_table_info;

-- Now, let's get a deduplicated list of all column names across temp tables
WITH all_temp_columns AS (
    SELECT DISTINCT
        column_name,
        data_type,
        COUNT(DISTINCT table_name) as table_count
    FROM INFORMATION_SCHEMA.columns 
    WHERE table_name LIKE 'temp%'
    GROUP BY column_name, data_type
    ORDER BY table_count DESC, column_name
)
SELECT 
    column_name,
    data_type,
    table_count as "number_of_tables"
FROM all_temp_columns;

-- Let's also get a detailed view of which columns appear in which tables
WITH column_usage AS (
    SELECT 
        table_name,
        column_name,
        data_type,
        ordinal_position
    FROM INFORMATION_SCHEMA.columns 
    WHERE table_name LIKE 'temp%'
    ORDER BY table_name, ordinal_position
)
SELECT 
    table_name,
    STRING_AGG(column_name, ', ' ORDER BY ordinal_position) as columns
FROM column_usage
GROUP BY table_name
ORDER BY table_name; 