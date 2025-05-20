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
    try:
        # Generate a unique table name
        temp_table_name = f"temp_csv_import_{int(time.time())}"
        
        # Read CSV header to get column names
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            # Count the actual number of columns in the first data row
            first_row = next(reader)
            num_columns = len(first_row)
            # Reset file pointer to start
            f.seek(0)
            next(reader)  # Skip header again
        
        # Filter out 'rownumber' from columns and ensure we have all columns
        columns = [f'"{col}" text' for col in header if col.lower() != 'rownumber']
        
        # Create temporary table with all columns as text
        create_table_sql = f"""
            CREATE {'TEMPORARY' if not keep_temp else ''} TABLE {temp_table_name} (
                {', '.join(columns)}
            ) {'ON COMMIT DROP' if not keep_temp else ''};
        """
        
        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            
            # Print table structure for debugging
            print(f"\nTable structure for {temp_table_name}:")
            cur.execute(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{temp_table_name}'
                ORDER BY ordinal_position;
            """)
            for col_name, data_type in cur.fetchall():
                print(f"  - {col_name}: {data_type}")
            
            # Copy CSV to import directory
            import_filename = f"import_{int(time.time())}_{os.path.basename(file_path)}"
            import_path = os.path.join('import', import_filename)
            os.makedirs('import', exist_ok=True)
            shutil.copy2(file_path, import_path)
            print(f"  - Copying {file_path} to import directory as {import_filename}")
            
            # Use COPY command for bulk loading with proper CSV format options
            copy_sql = f"""
                COPY {temp_table_name} FROM '/import/{import_filename}' 
                WITH (
                    FORMAT csv,
                    HEADER true,
                    DELIMITER ',',
                    QUOTE '"',
                    ESCAPE '"',
                    NULL ''
                );
            """
            print(f"  - Executing COPY command from /import/{import_filename}")
            cur.execute(copy_sql)
            print(f"  - Bulk loaded {cur.rowcount} rows into {temp_table_name}")
            
            # Clean up the temporary import file
            print(f"  - Cleaning up temporary import file: {import_path}")
            os.remove(import_path)
            print(f"  - Successfully removed temporary import file")
            
            # Get the merge SQL for debugging
            cur.execute("""
                SELECT get_merge_jsonb_sql(
                    %s,  -- temp table name
                    'id_bb_global',  -- ID column name
                    'csv_data',  -- target table name
                    'data',  -- JSONB column in target table
                    ARRAY['created_at', 'updated_at', 'filedate']  -- columns to exclude
                );
            """, (temp_table_name,))
            merge_sql = cur.fetchone()[0]
            print("\nGenerated merge SQL:")
            print(merge_sql)
            
            # Execute the merge
            cur.execute("""
                SELECT merge_jsonb_from_temp(
                    %s,  -- temp table name
                    'id_bb_global',  -- ID column name
                    'csv_data',  -- target table name
                    'data',  -- JSONB column in target table
                    ARRAY['created_at', 'updated_at', 'filedate']  -- columns to exclude
                );
            """, (temp_table_name,))
            
            conn.commit()
            return cur.rowcount
            
    except Exception as e:
        conn.rollback()
        raise Exception(f"Error processing {file_path}: {str(e)}")


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
