#!/usr/bin/env python3
"""
CSV to PostgreSQL Loader

This script loads data from CSV files into a PostgreSQL database.
Each row is stored with ID_BB_GLOBAL as the primary key and all other columns as a JSONB object.
If a row with the same ID_BB_GLOBAL already exists, new properties are merged into the existing JSONB.

Usage:
    python csv_to_postgres.py [--host HOST] [--port PORT] [--dbname DBNAME] 
                             [--user USER] [--password PASSWORD] 
                             [--table TABLE] [--limit LIMIT] 
                             csv_file [csv_file ...]

Arguments:
    csv_file            - One or more CSV files to load
    --host              - PostgreSQL host (default: localhost)
    --port              - PostgreSQL port (default: 5432)
    --dbname            - PostgreSQL database name (default: csvdata)
    --user              - PostgreSQL username (default: postgres)
    --password          - PostgreSQL password (default: Password123)
    --table             - Table name to load data into (default: csv_data)
    --limit             - Limit the number of rows to process per file (default: all)
    --help              - Show this help message
"""

import sys
import os
import csv
import json
import glob
import argparse
import psycopg2
from psycopg2.extras import Json
import time
from typing import Dict, List, Optional, Any, Iterable
import shutil


def create_table_if_not_exists(conn, table_name: str) -> None:
    """
    Create the table if it doesn't exist and create necessary indexes.
    
    Args:
        conn: Database connection
        table_name: Name of the table to create
    """
    with conn.cursor() as cur:
        # Create the main table
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id_bb_global TEXT PRIMARY KEY,
            data JSONB
        );
        """)
        
        # Create GIN indexes for the specified JSONB fields
        index_fields = [
            'ID_BB_GLOBAL_COMPANY',
            'ID_ISIN',
            'ID_CUSIP',
            'ID_SEDOL1',
            'ID_SEDOL2'
        ]
        
        for field in index_fields:
            index_name = f"idx_{table_name}_{field.lower()}"
            # First check if the index exists
            cur.execute("""
            SELECT 1 FROM pg_indexes 
            WHERE indexname = %s
            """, (index_name,))
            
            if not cur.fetchone():
                # Create GIN index on the specific JSONB field
                cur.execute(f"""
                CREATE INDEX {index_name} 
                ON {table_name} 
                USING GIN ((data -> '{field}'))
                """)
        
        conn.commit()


def create_temp_table(conn, columns: List[str]) -> str:
    """
    Create a temporary table with the same structure as the CSV file.
    
    Args:
        conn: Database connection
        columns: List of column names from the CSV
        
    Returns:
        Name of the temporary table
    """
    temp_table = f"temp_csv_import_{int(time.time())}"
    
    with conn.cursor() as cur:
        # Create temporary table with all columns as TEXT
        columns_sql = ", ".join([f"{col.lower()} TEXT" for col in columns])
        cur.execute(f"""
        CREATE TEMPORARY TABLE {temp_table} (
            {columns_sql}
        ) ON COMMIT DROP;
        """)
        conn.commit()
    
    return temp_table


def process_csv_file(
    conn,
    file_path: str,
    table_name: str,
    limit: Optional[int] = None,
    debug: bool = False,
    keep_temp: bool = False
) -> int:
    # Generate a unique temporary table name for this import
    temp_table = f"temp_csv_import_{int(time.time())}"
    
    # Start a new transaction
    conn.rollback()  # Ensure we start with a clean transaction
    conn.autocommit = False
    
    # Clean up any existing temporary table with the same name
    with conn.cursor() as cleanup_cur:
        cleanup_cur.execute(f"DROP TABLE IF EXISTS {temp_table} CASCADE")
    conn.commit()
    
    start_time = time.time()
    file_size = os.path.getsize(file_path)
    
    print(f"Processing file: {file_path}")
    print(f"File size: {file_size / (1024 * 1024):.2f} MB")
    
    try:
        # First, read the CSV header to get column names
        with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            columns = [col.strip() for col in next(reader)]  # Clean column names
            
            # Check if ID_BB_GLOBAL column exists
            if 'ID_BB_GLOBAL' not in columns:
                print(f"Error: ID_BB_GLOBAL column not found in {file_path}")
                return 0
            
            # Create table (temporary or regular based on keep_temp flag)
            with conn.cursor() as cur:
                # List of PostgreSQL reserved keywords that need quoting
                reserved_keywords = {
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
                }
                
                # Convert column names to lowercase and quote if they are reserved keywords
                columns_sql = ", ".join([
                    f'"{col.lower()}" TEXT' if col.lower() in reserved_keywords 
                    else f"{col.lower()} TEXT" 
                    for col in columns
                ])
                
                if keep_temp:
                    cur.execute(f"""
                    CREATE TABLE {temp_table} (
                        {columns_sql}
                    );
                    """)
                    print(f"  - Created regular table {temp_table} (will be kept)")
                else:
                    cur.execute(f"""
                    CREATE TEMPORARY TABLE {temp_table} (
                        {columns_sql}
                    ) ON COMMIT DROP;
                    """)
                    print(f"  - Created temporary table {temp_table} (will be dropped)")
                
                # Verify table was created
                cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
                """, (temp_table,))
                table_exists = cur.fetchone()[0]
                if not table_exists:
                    raise Exception(f"Failed to create table {temp_table}")
                
                # Print table structure for debugging
                print(f"\nTable structure for {temp_table}:")
                cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position;
                """, (temp_table,))
                columns = cur.fetchall()
                for col in columns:
                    print(f"  - {col[0]}: {col[1]}")
                print()
                
                # Copy file to import directory for PostgreSQL to access
                import_filename = f"import_{int(time.time())}_{os.path.basename(file_path)}"
                import_path = os.path.join('import', import_filename)
                print(f"  - Copying {file_path} to import directory as {import_filename}")
                shutil.copy2(file_path, import_path)
                print(f"  - File copied successfully to {import_path}")
                
                try:
                    # Use COPY command to bulk load data directly into temp table
                    print(f"  - Executing COPY command from /import/{import_filename}")
                    cur.execute(f"""
                    COPY {temp_table} FROM '/import/{import_filename}' WITH (FORMAT CSV, HEADER, ENCODING 'UTF-8')
                    """)
                    
                    # Verify data was loaded
                    cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
                    row_count = cur.fetchone()[0]
                    print(f"  - Bulk loaded {row_count:,} rows into {temp_table}")
                    
                    # Print the generated SQL for the merge function if debug is enabled
                    if debug:
                        try:
                            cur.execute("""
                                SELECT get_merge_jsonb_sql(
                                    %s,  -- temp table name
                                    'id_bb_global',  -- ID column name
                                    %s,  -- target table name
                                    'data',  -- JSONB column in target table
                                    ARRAY['created_at', 'updated_at', 'rownumber', 'filedate']  -- columns to exclude
                                );
                            """, (temp_table, table_name))
                            merge_sql = cur.fetchone()[0]
                            print("\nGenerated SQL for merge_jsonb_from_temp:\n")
                            print(merge_sql)
                        except Exception as e:
                            print(f"  - Warning: Could not generate merge SQL: {e}")
                finally:
                    # Clean up the imported file
                    try:
                        print(f"  - Cleaning up temporary import file: {import_path}")
                        os.remove(import_path)
                        print(f"  - Successfully removed temporary import file")
                    except Exception as e:
                        print(f"  - Warning: Could not remove temporary import file: {e}")
                
                # Get the list of non-ID columns for JSON building
                non_id_columns = [col for col in columns if col != 'ID_BB_GLOBAL']
                
                # Process in chunks to avoid parameter limit issues
                chunk_size = 50
                json_parts = []
                for i in range(0, len(non_id_columns), chunk_size):
                    chunk = non_id_columns[i:i + chunk_size]
                    json_parts.append(
                        "jsonb_build_object(" + 
                        ", ".join(f"'{col}', NULLIF(\"{col}\", '')" for col in chunk) + 
                        ")"
                    )
                
                json_build_expr = " || ".join(json_parts) if json_parts else "'{}'::jsonb"
                
                # First, handle inserts for new records
                cur.execute(f"""
                INSERT INTO {table_name} (id_bb_global, data)
                SELECT 
                    "ID_BB_GLOBAL",
                    {json_build_expr} as new_data
                FROM {temp_table} t
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} 
                    WHERE id_bb_global = t."ID_BB_GLOBAL"
                );
                """)
                
                # For updates, we'll process each row individually using a cursor
                # to ensure proper JSONB merging
                with conn.cursor() as update_cur:
                    # First, check if the target table exists and has data
                    update_cur.execute(f"""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables 
                            WHERE table_name = %s
                        )
                    """, (table_name,))
                    table_exists = update_cur.fetchone()[0]
                    
                    if not table_exists:
                        print(f"  - Target table {table_name} does not exist, no updates needed")
                        return 0
                        
                    # Get all rows from the temp table
                    update_cur.execute(f"""
                        SELECT * FROM {temp_table}
                    """)
                    
                    # Get column names from the temp table
                    update_cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s
                        AND column_name != 'ID_BB_GLOBAL'
                        ORDER BY ordinal_position
                    """, (temp_table,))
                    update_columns = [row[0] for row in update_cur.fetchall()]
                    
                    print(f"  - Found {len(update_columns)} columns to process in updates")
                    
                    # Get all rows from the temp table and fetch them all at once
                    update_cur.execute(f"""
                        SELECT * FROM {temp_table}
                    """)
                    
                    # Get column names for the temp table
                    colnames = [desc[0] for desc in update_cur.description] if update_cur.description else []
                    
                    # Fetch all rows at once to avoid cursor exhaustion
                    rows = update_cur.fetchall()
                    print(f"  - Found {len(rows)} rows to process")
                    
                    updated_count = 0
                    for row in rows:
                        try:
                            row_dict = dict(zip(colnames, row))
                            id_bb_global = row_dict.pop('ID_BB_GLOBAL')
                            
                            # Create a JSONB object with non-null values
                            update_data = {
                                k: v for k, v in row_dict.items() 
                                if v is not None and v != '' and k in update_columns
                            }
                            
                            if not update_data:
                                continue
                            
                            # Special debug for the problematic ID
                            is_problematic_id = (id_bb_global == 'LgyMu6dSbEP4')
                            
                            if is_problematic_id and debug:
                                print("\n=== DEBUG: Found problematic ID LgyMu6dSbEP4 ===")
                                print("Raw row data:", row)
                                print("Update data prepared:", update_data)
                            
                            # Get current data for the record
                            with conn.cursor() as fetch_cur:
                                fetch_cur.execute(
                                    f"""
                                    SELECT data, id_bb_global 
                                    FROM {table_name} 
                                    WHERE id_bb_global = %s
                                    FOR UPDATE
                                    """,
                                    (id_bb_global,)
                                )
                                current_row = fetch_cur.fetchone()
                                
                                if not current_row:
                                    print(f"  - WARNING: Record {id_bb_global} not found in {table_name}")
                                    continue
                                    
                                current_data = dict(current_row[0] or {})  # Ensure we have a mutable copy
                                
                                if is_problematic_id and debug:
                                    print("\n=== DEBUG: BEFORE MERGE ===")
                                    print("ID:", id_bb_global)
                                    print("Current data in DB:", json.dumps(current_data, indent=4))
                                    print("New data to merge:", json.dumps(update_data, indent=4))
                            
                            # Create a new dictionary with all fields from current data
                            merged_data = {}
                            
                            # First add all fields from current data
                            for k, v in current_data.items():
                                if v is not None and v != '':
                                    merged_data[k] = v
                            
                            # Then update with new data (this will override existing fields and add new ones)
                            for k, v in update_data.items():
                                if v is not None and v != '':
                                    merged_data[k] = v
                            
                            if is_problematic_id and debug:
                                print("\n=== DEBUG: AFTER MERGE ===")
                                print("Merged data:", json.dumps(merged_data, indent=4))
                                print(f"Fields in current data: {len(current_data)}")
                                print(f"Fields in update data: {len(update_data)}")
                                print(f"Fields in merged data: {len(merged_data)}")
                            
                            # Use a new cursor for the update
                            with conn.cursor() as upsert_cur:
                                try:
                                    # First try to update the existing record
                                    upsert_cur.execute(
                                        f"""
                                        UPDATE {table_name}
                                        SET data = %s::jsonb
                                        WHERE id_bb_global = %s
                                        RETURNING id_bb_global, data
                                        """,
                                        (json.dumps(merged_data), id_bb_global)
                                    )
                                    
                                    # If no rows were updated, try to insert
                                    if upsert_cur.rowcount == 0:
                                        if is_problematic_id:
                                            print("\n=== DEBUG: NO ROWS UPDATED, TRYING INSERT ===")
                                        upsert_cur.execute(
                                            f"""
                                            INSERT INTO {table_name} (id_bb_global, data)
                                            VALUES (%s, %s::jsonb)
                                            RETURNING id_bb_global, data
                                            """,
                                            (id_bb_global, json.dumps(merged_data))
                                        )
                                    
                                    updated_count += 1
                                    if updated_count % 100 == 0:
                                        print(f"  - Updated {updated_count} records...")
                                        
                                except Exception as e:
                                    print(f"Error processing record {id_bb_global}: {str(e)}")
                                    conn.rollback()
                                    continue
                                
                                # Commit after each successful operation
                                conn.commit()
                                
                        except Exception as e:
                            print(f"Error processing row: {str(e)}")
                            conn.rollback()
                            continue
                    
                    # Get number of rows processed from the temp table
                    try:
                        with conn.cursor() as count_cur:
                            count_cur.execute(f"SELECT COUNT(*) FROM {temp_table}")
                            rows_processed = count_cur.fetchone()[0]
                            print(f"  - Verified {rows_processed:,} rows in {temp_table}")
                    except Exception as e:
                        print(f"  - Warning: Could not get row count: {e}")
                        rows_processed = updated_count  # Fall back to updated_count
                    
                    # Commit the transaction
                    conn.commit()
                    print(f"  - Successfully updated {updated_count} rows")
                    
                    # Log processing time
                    processing_time = time.time() - start_time
                    print(f"  - Processed {rows_processed:,} rows in {processing_time:.2f} seconds")
                    
                    # Clean up the temporary table if not keeping it
                    if not keep_temp:
                        try:
                            with conn.cursor() as cleanup_cur:
                                cleanup_cur.execute(f"DROP TABLE IF EXISTS {temp_table} CASCADE")
                                conn.commit()
                                print(f"  - Dropped temporary table {temp_table}")
                        except Exception as e:
                            print(f"  - Warning: Could not drop temporary table: {e}")
                            conn.rollback()
                    else:
                        print(f"  - Kept table {temp_table} as requested")
                        # Verify table still exists
                        with conn.cursor() as verify_cur:
                            verify_cur.execute(f"""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = %s
                            );
                            """, (temp_table,))
                            still_exists = verify_cur.fetchone()[0]
                            if still_exists:
                                print(f"  - Verified table {temp_table} still exists")
                            else:
                                print(f"  - WARNING: Table {temp_table} no longer exists!")
                    
                    # Debug output for specific IDs
                    if debug:
                        debug_ids = ['LgyMu6dSbEP4', '9Pt8sJtV2U2b']
                        for debug_id in debug_ids:
                            try:
                                with conn.cursor() as debug_cur:
                                    debug_cur.execute(
                                        f"""
                                        SELECT data FROM {table_name}
                                        WHERE id_bb_global = %s
                                        """,
                                        (debug_id,)
                                    )
                                    result = debug_cur.fetchone()
                                    if result:
                                        print(f"\nDebug - Current data for {debug_id}:")
                                        print(json.dumps(result[0], indent=4))
                            except Exception as e:
                                print(f"  - Error fetching debug data for {debug_id}: {e}")
                    
                    return rows_processed
                
            elapsed = time.time() - start_time
            print(f"Completed processing {rows_processed:,} rows from {file_path} in {elapsed:.2f} seconds")
            return rows_processed
            
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        if 'conn' in locals():
            conn.rollback()
        return 0


def expand_file_patterns(patterns: Iterable[str]) -> List[str]:
    """
    Expand file patterns to matching files.
    
    Args:
        patterns: List of file paths or patterns (can contain wildcards)
        
    Returns:
        List of matching file paths, sorted alphabetically
    """
    files = []
    for pattern in patterns:
        # Expand the pattern to matching files
        matches = glob.glob(pattern, recursive=True)
        if not matches:
            print(f"Warning: No files match pattern: {pattern}")
        files.extend(matches)
    
    # Remove duplicates and sort for consistent processing order
    return sorted(list(set(files)))


def main():
    parser = argparse.ArgumentParser(
        description="Load CSV files into PostgreSQL. Supports wildcards (e.g., 'data/*.csv')",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load specific files
  python csv_to_postgres.py file1.csv file2.csv
  
  # Load all CSV files in a directory
  python csv_to_postgres.py "data/*.csv"
  
  # Recursively load all CSV files in a directory and subdirectories
  python csv_to_postgres.py "data/**/*.csv"
  
  # Load files matching multiple patterns
  python csv_to_postgres.py "data/2023-*.csv" "backups/*.csv"
  
  # With database options
  python csv_to_postgres.py "data/*.csv" --dbname mydb --user myuser
  
  # With debug output
  python csv_to_postgres.py "data/*.csv" --debug
  
  # Keep temporary tables after loading
  python csv_to_postgres.py "data/*.csv" --keep-temp
""")
    parser.add_argument(
        "csv_files", 
        nargs='+', 
        help="CSV files or patterns to load (supports wildcards like 'data/*.csv')"
    )
    parser.add_argument("--host", default="localhost", help="PostgreSQL host (default: localhost)")
    parser.add_argument("--port", type=int, default=5432, help="PostgreSQL port (default: 5432)")
    parser.add_argument("--dbname", default="csvdata", help="PostgreSQL database name (default: csvdata)")
    parser.add_argument("--user", default="postgres", help="PostgreSQL username (default: postgres)")
    parser.add_argument("--password", default="Password123", help="PostgreSQL password (default: Password123)")
    parser.add_argument("--table", default="csv_data", help="Table name to load data into (default: csv_data)")
    parser.add_argument("--limit", type=int, help="Limit the number of rows to process per file")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
    parser.add_argument("--keep-temp", action="store_true", help="Keep temporary tables after loading")
    
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print(__doc__)
        sys.exit(0)
    
    args = parser.parse_args()
    
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password
        )
        
        # Create table if it doesn't exist
        create_table_if_not_exists(conn, args.table)
        
        total_rows = 0
        start_time = time.time()
        
        # Expand file patterns and process each matching file
        file_paths = expand_file_patterns(args.csv_files)
        
        if not file_paths:
            print("Error: No matching files found.")
            return
            
        print(f"Found {len(file_paths)} files to process")
        
        # Process each CSV file
        for i, file_path in enumerate(file_paths, 1):
            print(f"\nProcessing file {i} of {len(file_paths)}: {file_path}")
            try:
                rows = process_csv_file(conn, file_path, args.table, args.limit, args.debug, args.keep_temp)
                total_rows += rows
            except Exception as e:
                print(f"Error processing {file_path}: {str(e)}")
                continue
        
        total_time = time.time() - start_time
        print(f"Total: {total_rows:,} rows processed in {total_time:.2f} seconds")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()


if __name__ == "__main__":
    main()
