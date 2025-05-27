#!/usr/bin/env python3
"""
CSV to Temporary Tables Loader

This script loads data from CSV files into temporary PostgreSQL tables.
Each CSV is loaded into a separate temporary table with a random name.
The table structure matches the CSV columns exactly.
"""

import os
import sys
import csv
import time
import random
import string
import logging
import psycopg2
from typing import List, Dict, Optional, Tuple
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def generate_table_name(csv_path: str, prefix: str = 'temp_', random_length: int = 4) -> str:
    """
    Generate a table name based on the CSV filename and a random string.
    
    Args:
        csv_path: Path to the CSV file
        prefix: Prefix for the table name
        random_length: Length of the random string to append
        
    Returns:
        str: A table name in the format: prefix_basename_random
    """
    # Get the base filename without extension
    base_name = os.path.splitext(os.path.basename(csv_path))[0]
    
    # Generate a random string
    chars = string.ascii_lowercase + string.digits
    random_suffix = ''.join(random.choices(chars, k=random_length))
    
    # Clean the base name to be a valid identifier
    clean_name = ''.join(c if c.isalnum() else '_' for c in base_name)
    
    # Combine components
    return f"{prefix}{clean_name}_{random_suffix}".lower()

def get_csv_columns(file_path: str) -> List[str]:
    """Read the first line of a CSV file to get column names."""
    try:
        with open(file_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            return [col.strip() for col in next(reader)]
    except Exception as e:
        logging.error(f"Error reading CSV columns from {file_path}: {e}")
        return []

def create_temp_table(conn, table_name: str, columns: List[str]) -> bool:
    """Create a temporary table with the given columns."""
    with conn.cursor() as cur:
        try:
            # Create column definitions with TEXT type for all columns
            columns_sql = ", ".join([f'\"{col}\" TEXT' for col in columns])
            
            # Create the temporary table with a simpler approach
            create_sql = f"""
                DROP TABLE IF EXISTS \"{table_name}" CASCADE;
                CREATE /*TEMPORARY*/ TABLE \"{table_name}" (
                    {columns_sql}
                );
            """
            
            logging.info(f"Creating table with SQL:\n{create_sql}")
            cur.execute(create_sql)
            conn.commit()
            logging.info(f"Successfully created table: {table_name}")
            return True
        except Exception as e:
            conn.rollback()
            logging.error(f"Error creating table {table_name}: {e}")
            return False

def load_csv_to_temp_table(conn, file_path: str, table_name: str) -> Tuple[bool, str]:
    """Load data from CSV into a temporary table."""
    try:
        print(f"\nProcessing file: {file_path}")
        
        # Get column names from CSV
        columns = get_csv_columns(file_path)
        print(f"Found columns: {', '.join(columns[:5])}...")
        
        # Create the temporary table
        print(f"Creating temporary table: {table_name}")
        if not create_temp_table(conn, table_name, columns):
            return False, f"Failed to create table {table_name}"
        
        # Verify table exists
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                return False, f"Table {table_name} was not created successfully"
        
        # Use COPY command to load data
        with conn.cursor() as cur, open(file_path, 'r', encoding='utf-8') as f:
            # Skip header row
            next(f)
            
            # Build the COPY command with proper quoting
            columns_quoted = [f'\"{col}\"' for col in columns]
            copy_sql = f"""
                COPY \"{table_name}" ({','.join(columns_quoted)}) 
                FROM STDIN 
                WITH (FORMAT csv, DELIMITER ',');
            """
            
            print(f"Executing COPY command...")
            cur.copy_expert(copy_sql, f)
            
            # Verify data was loaded
            cur.execute(f'SELECT COUNT(*) FROM \"{table_name}"')
            count = cur.fetchone()[0]
            print(f"Loaded {count} rows into {table_name}")
            
        conn.commit()
        return True, f"Successfully loaded {os.path.basename(file_path)} into {table_name}"
        
    except Exception as e:
        conn.rollback()
        return False, f"Error loading {file_path}: {str(e)}"

def process_csv_files(conn, file_paths: List[str]) -> Dict[str, str]:
    """Process multiple CSV files into temporary tables."""
    results = {}
    
    for file_path in file_paths:
        if not os.path.isfile(file_path):
            results[file_path] = f"Error: File not found"
            continue
            
        # Generate a table name based on the CSV filename and a random string
        table_name = generate_table_name(file_path, 'temp_')
        
        # Load the CSV into a temporary table
        success, message = load_csv_to_temp_table(conn, file_path, table_name)
        
        if success:
            results[file_path] = f"Loaded into table: {table_name}"
            print(f"{file_path} → {table_name}")
        else:
            results[file_path] = message
    
    return results

def main():
    """Main function to handle command line arguments and execution."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Load CSV files into temporary PostgreSQL tables')
    parser.add_argument('csv_files', nargs='+', help='CSV files to load')
    parser.add_argument('--host', default='localhost', help='PostgreSQL host')
    parser.add_argument('--port', default=5432, type=int, help='PostgreSQL port')
    parser.add_argument('--dbname', default='csvdata', help='Database name')
    parser.add_argument('--user', default='postgres', help='Database user')
    parser.add_argument('--password', default='Password123', help='Database password')
    
    args = parser.parse_args()
    
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        
        print(f"Loading {len(args.csv_files)} CSV files into temporary tables...\n")
        
        # Process the CSV files
        results = process_csv_files(conn, args.csv_files)
        
        # Print results
        print("\nProcessing complete!")
        print("-" * 50)
        for file_path, message in results.items():
            print(f"{os.path.basename(file_path)}: {message}")
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
