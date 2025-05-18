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


def process_csv_file(
    conn,
    file_path: str,
    table_name: str,
    limit: Optional[int] = None
) -> int:
    """
    Process a CSV file and load it into PostgreSQL.
    
    Args:
        conn: PostgreSQL connection
        file_path: Path to CSV file
        table_name: Table name to load data into
        limit: Maximum number of rows to process
        
    Returns:
        Number of rows processed
    """
    start_time = time.time()
    file_size = os.path.getsize(file_path)
    rows_processed = 0
    
    print(f"Processing file: {file_path}")
    print(f"File size: {file_size / (1024 * 1024):.2f} MB")
    
    try:
        with open(file_path, 'r', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Check if ID_BB_GLOBAL column exists
            if 'ID_BB_GLOBAL' not in reader.fieldnames:
                print(f"Error: ID_BB_GLOBAL column not found in {file_path}")
                return 0
            
            batch_size = 1000
            batch = []
            
            for row in reader:
                # Extract ID_BB_GLOBAL and create JSON object from all columns
                id_bb_global = row.get('ID_BB_GLOBAL')
                if not id_bb_global:
                    continue  # Skip rows without ID_BB_GLOBAL
                
                # Add row to batch
                batch.append((id_bb_global, row))
                
                # Process batch if it reaches batch_size
                if len(batch) >= batch_size:
                    insert_batch(conn, table_name, batch)
                    rows_processed += len(batch)
                    batch = []
                    
                    # Print progress
                    if rows_processed % 10000 == 0:
                        elapsed = time.time() - start_time
                        print(f"Processed {rows_processed:,} rows ({elapsed:.2f} seconds)")
                
                # Stop if limit is reached
                if limit is not None and rows_processed >= limit:
                    break
            
            # Process remaining batch
            if batch:
                insert_batch(conn, table_name, batch)
                rows_processed += len(batch)
            
            elapsed = time.time() - start_time
            print(f"Completed processing {rows_processed:,} rows from {file_path} in {elapsed:.2f} seconds")
            
            return rows_processed
            
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return 0


def insert_batch(conn, table_name: str, batch: List[tuple]) -> None:
    """
    Insert or merge a batch of rows into the database.
    
    Args:
        conn: PostgreSQL connection
        table_name: Table name to insert into
        batch: List of (id_bb_global, row_data) tuples
    """
    with conn.cursor() as cur:
        # First, insert or update each row with the new data
        for id_bb_global, row_data in batch:
            # Remove ID_BB_GLOBAL from row_data as it's already the primary key
            row_data = {k: v for k, v in row_data.items() if k != 'ID_BB_GLOBAL'}
            
            # Convert the row data to a JSON string for the SQL query
            row_json = json.dumps(row_data)
            
            # First, insert or update the record with the new data
            # If the record exists, we'll merge the data in the next step
            cur.execute(f"""
            INSERT INTO {table_name} (id_bb_global, data)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (id_bb_global) 
            DO UPDATE SET data = {table_name}.data || EXCLUDED.data
            RETURNING id_bb_global, data;
            """, (id_bb_global, row_json))
            
            # Now, update the record by merging the existing data with the new data
            # This ensures that only the fields that exist in the new data are updated
            # and all other fields remain unchanged
            for key, value in row_data.items():
                if value is not None:  # Only update non-NULL values
                    cur.execute(f"""
                    UPDATE {table_name}
                    SET data = jsonb_set(
                        COALESCE(data, '{{}}'::jsonb),
                        '{{{key}}}',
                        %s::jsonb,
                        true
                    )
                    WHERE id_bb_global = %s;
                    """, (json.dumps(value), id_bb_global))
        
        conn.commit()


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
                rows = process_csv_file(conn, file_path, args.table, args.limit)
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
