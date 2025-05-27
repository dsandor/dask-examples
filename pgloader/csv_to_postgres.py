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
                             [--import-root DIR] [--include-regex PATTERN]
                             [--exclude-regex PATTERN]
                             [csv_file [csv_file ...]]

Arguments:
    csv_file            - One or more CSV files to load
    --host              - PostgreSQL host (default: localhost)
    --port              - PostgreSQL port (default: 5432)
    --dbname            - PostgreSQL database name (default: csvdata)
    --user              - PostgreSQL username (default: postgres)
    --password          - PostgreSQL password (default: Password123)
    --table             - Table name to load data into (default: csv_data)
    --limit             - Limit the number of rows to process per file (default: all)
    --import-root       - Root directory to search for CSV files
    --include-regex     - Regex pattern to match directory names to include
    --exclude-regex     - Regex pattern to match directory names to exclude
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
import re
import gzip
import hashlib
from typing import Dict, List, Optional, Any, Iterable, Tuple
import shutil
from datetime import datetime


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
        # Create temporary table with all columns as TEXT, preserving case
        columns_sql = ", ".join([f'"{col}" TEXT' for col in columns])
        cur.execute(f"""
        CREATE TEMPORARY TABLE {temp_table} (
            {columns_sql}
        ) ON COMMIT DROP;
        """)
        conn.commit()
    
    return temp_table


def process_csv_file(csv_file, conn, keep_temp=False):
    """Process a single CSV file and load it into PostgreSQL."""
    try:
        # Generate a unique table name with a short hash of the filename to avoid collisions
        filename_hash = hashlib.md5(os.path.basename(csv_file).encode()).hexdigest()[:8]
        temp_table_name = f"temp_csv_import_{int(time.time())}_{filename_hash}"
        
        # Read CSV to determine column count and names
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            # Read first few rows to determine max columns
            max_columns = len(header)
            sample_rows = []
            for _ in range(5):  # Read up to 5 rows to determine max columns
                try:
                    row = next(reader)
                    max_columns = max(max_columns, len(row))
                    sample_rows.append(row)
                except StopIteration:
                    break
            
            # If we have more columns than header, extend header with generic names
            if max_columns > len(header):
                print(f"  - Warning: CSV has {max_columns} columns but header only has {len(header)} columns")
                print(f"  - Extending header with generic column names")
                header.extend([f'column_{i+1}' for i in range(len(header), max_columns)])
            
            # Reset file pointer to start
            f.seek(0)
            next(reader)  # Skip header again
        
        # Create columns list preserving case
        columns = [f'"{col}" text' for col in header]
        
        # Create temporary table with all columns as text
        create_table_sql = f"""
            CREATE {'TEMPORARY' if not keep_temp else ''} TABLE {temp_table_name} (
                {', '.join(columns)}
            ) {'ON COMMIT DROP' if not keep_temp else ''};
        """
        
        print(f"  - Creating {'temporary' if not keep_temp else 'regular'} table {temp_table_name}")
        
        # Close any existing transaction
        if not conn.autocommit:
            conn.rollback()
        
        # Set autocommit for table creation and data loading
        conn.autocommit = True
        
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
            import_filename = f"import_{int(time.time())}_{os.path.basename(csv_file)}"
            import_path = os.path.join('import', import_filename)
            os.makedirs('import', exist_ok=True)
            shutil.copy2(csv_file, import_path)
            print(f"  - Copying {csv_file} to import directory as {import_filename}")
            
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
                    'ID_BB_GLOBAL',  -- ID column name (using exact case from CSV)
                    'csv_data',  -- target table name
                    'data',  -- JSONB column in target table
                    ARRAY['created_at', 'updated_at', 'filedate', 'rownumber']  -- columns to exclude
                );
            """, (temp_table_name,))
            merge_sql = cur.fetchone()[0]
            print("\nGenerated merge SQL:")
            print(merge_sql)
            
            # Disable autocommit for the merge operation
            conn.autocommit = False
            
            try:
                # Execute the merge in a transaction
                print("  - Starting merge transaction...")
                cur.execute("""
                    SELECT merge_jsonb_from_temp(
                        %s,  -- temp table name
                        'ID_BB_GLOBAL',  -- ID column name (using exact case from CSV)
                        'csv_data',  -- target table name
                        'data',  -- JSONB column in target table
                        ARRAY['created_at', 'updated_at', 'filedate', 'rownumber']  -- columns to exclude
                    );
                """, (temp_table_name,))
                
                # Commit the merge transaction
                conn.commit()
                print("  - Merge transaction committed successfully")
                
            except Exception as e:
                print(f"  - Error during merge: {str(e)}")
                print("  - Rolling back merge transaction but keeping temp table")
                conn.rollback()
                raise Exception(f"Error during merge: {str(e)}")
            
            # Verify table still exists if keep_temp is True
            if keep_temp:
                cur.execute(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
                """, (temp_table_name,))
                still_exists = cur.fetchone()[0]
                if still_exists:
                    print(f"  - Verified table {temp_table_name} still exists")
                else:
                    print(f"  - WARNING: Table {temp_table_name} no longer exists!")
            
            return cur.rowcount
            
    except Exception as e:
        if not conn.autocommit:
            conn.rollback()
        raise Exception(f"Error processing {csv_file}: {str(e)}")


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


def find_latest_csv_gz_file(directory: str) -> Optional[str]:
    """
    Find the most recent CSV.GZ file in a directory by examining the filename
    which includes a date in the format YYYYMMDD.
    
    Args:
        directory: Directory to search for CSV.GZ files
        
    Returns:
        Path to the most recent CSV.GZ file, or None if no files found
    """
    # Look for files with pattern *YYYYMMDD*.csv.gz
    pattern = re.compile(r'.*?(\d{8}).*?\.csv\.gz$', re.IGNORECASE)
    latest_file = None
    latest_date = None
    
    for filename in os.listdir(directory):
        match = pattern.match(filename)
        if match:
            try:
                date_str = match.group(1)
                file_date = datetime.strptime(date_str, '%Y%m%d')
                
                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = os.path.join(directory, filename)
            except ValueError:
                # Skip files with invalid date format
                continue
    
    if latest_file:
        print(f"Found latest CSV.GZ file: {latest_file} (date: {latest_date.strftime('%Y-%m-%d')})") 
    else:
        print(f"No CSV.GZ files with date format YYYYMMDD found in {directory}")
        
    return latest_file


def extract_csv_gz(gz_file: str, output_dir: str = './import') -> str:
    """
    Extract a gzipped CSV file to the specified output directory.
    If the file already exists in the output directory, skip extraction.
    
    Args:
        gz_file: Path to the gzipped CSV file
        output_dir: Directory to extract the file to
        
    Returns:
        Path to the extracted CSV file
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # Create output filename by removing .gz extension
    base_name = os.path.basename(gz_file)
    if base_name.lower().endswith('.gz'):
        output_name = base_name[:-3]  # Remove .gz extension
    else:
        output_name = f"extracted_{base_name}"
        
    output_path = os.path.join(output_dir, output_name)
    
    # Check if file already exists
    if os.path.exists(output_path):
        print(f"File already exists: {output_path}. Skipping extraction.")
        return output_path
    
    print(f"Extracting {gz_file} to {output_path}")
    
    try:
        with gzip.open(gz_file, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"Successfully extracted to {output_path}")
        return output_path
    except Exception as e:
        print(f"Error extracting {gz_file}: {str(e)}")
        raise


def find_matching_directories(root_dir: str, include_pattern: Optional[str], exclude_pattern: Optional[str]) -> List[str]:
    """
    Find directories under root_dir that match include_pattern but not exclude_pattern.
    
    Args:
        root_dir: Root directory to search
        include_pattern: Regex pattern for directories to include (None means include all)
        exclude_pattern: Regex pattern for directories to exclude (None means exclude none)
        
    Returns:
        List of matching directory paths
    """
    if not os.path.isdir(root_dir):
        print(f"Error: {root_dir} is not a valid directory")
        return []
        
    include_regex = re.compile(include_pattern) if include_pattern else None
    exclude_regex = re.compile(exclude_pattern) if exclude_pattern else None
    
    matching_dirs = []
    
    for item in os.listdir(root_dir):
        item_path = os.path.join(root_dir, item)
        
        if not os.path.isdir(item_path):
            continue
            
        # Check if directory matches include pattern
        include_match = True if include_regex is None else bool(include_regex.search(item))
        
        # Check if directory matches exclude pattern
        exclude_match = False if exclude_regex is None else bool(exclude_regex.search(item))
        
        if include_match and not exclude_match:
            matching_dirs.append(item_path)
            
    return sorted(matching_dirs)


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
  
  # Use import-root with directory patterns
  python csv_to_postgres.py --import-root /data/feeds --include-regex "company_.*" --exclude-regex ".*_test"
  
  # Process latest CSV.GZ files from matching directories
  python csv_to_postgres.py --import-root /data/feeds --include-regex "finance_.*"
""")
    parser.add_argument(
        "csv_files", 
        nargs='*',  # Changed from '+' to '*' to make it optional when using --import-root
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
    parser.add_argument("--import-root", help="Root directory to search for CSV.GZ files")
    parser.add_argument("--include-regex", help="Regex pattern to match directory names to include")
    parser.add_argument("--exclude-regex", help="Regex pattern to match directory names to exclude")
    
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print(__doc__)
        sys.exit(0)
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.csv_files and not args.import_root:
        print("Error: Either csv_files or --import-root must be specified")
        sys.exit(1)
    
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
        file_paths = []
        
        # Handle import-root if specified
        if args.import_root:
            print(f"Searching for directories in {args.import_root}")
            print(f"Include pattern: {args.include_regex or 'None (include all)'}")
            print(f"Exclude pattern: {args.exclude_regex or 'None (exclude none)'}")
            
            matching_dirs = find_matching_directories(
                args.import_root, 
                args.include_regex, 
                args.exclude_regex
            )
            
            print(f"Found {len(matching_dirs)} matching directories")
            
            # Find the latest CSV.GZ file in each matching directory
            for dir_path in matching_dirs:
                print(f"\nSearching for latest CSV.GZ file in: {dir_path}")
                latest_file = find_latest_csv_gz_file(dir_path)
                
                if latest_file:
                    # Extract the CSV.GZ file
                    try:
                        extracted_path = extract_csv_gz(latest_file)
                        file_paths.append(extracted_path)
                    except Exception as e:
                        print(f"Error extracting {latest_file}: {str(e)}")
        
        # Handle explicit CSV files if specified
        if args.csv_files:
            # Expand file patterns and process each matching file
            explicit_files = expand_file_patterns(args.csv_files)
            file_paths.extend(explicit_files)
        
        if not file_paths:
            print("Error: No files to process.")
            return
            
        print(f"\nFound {len(file_paths)} files to process")
        
        # Process each CSV file
        for i, file_path in enumerate(file_paths, 1):
            print(f"\nProcessing file {i} of {len(file_paths)}: {file_path}")
            try:
                rows = process_csv_file(file_path, conn, args.keep_temp)
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
