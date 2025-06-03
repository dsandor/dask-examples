#!/usr/bin/env python3

import psycopg2
import pandas as pd
from typing import List, Set
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'missing_ids_check_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)

def get_db_connection(db_params: dict) -> psycopg2.extensions.connection:
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**db_params)
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

def get_temp_tables(conn: psycopg2.extensions.connection) -> List[str]:
    """Get all tables that start with 'temp_'."""
    query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    AND table_name LIKE 'temp_%'
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logging.error(f"Failed to get temp tables: {e}")
        raise

def get_missing_ids(conn: psycopg2.extensions.connection, temp_table: str) -> Set[str]:
    """Find IDs in temp table that don't exist in csv_data table."""
    query = f"""
    SELECT DISTINCT t."ID_BB_GLOBAL"
    FROM {temp_table} t
    LEFT JOIN csv_data c ON t."ID_BB_GLOBAL" = c.id_bb_global
    WHERE c.id_bb_global IS NULL
    AND t."ID_BB_GLOBAL" IS NOT NULL
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return {row[0] for row in cur.fetchall()}
    except Exception as e:
        logging.error(f"Failed to get missing IDs for table {temp_table}: {e}")
        raise

def main():
    # Database connection parameters
    db_params = {
        'dbname': 'your_database',
        'user': 'your_user',
        'password': 'your_password',
        'host': 'your_host',
        'port': '5432'
    }

    try:
        # Connect to database
        conn = get_db_connection(db_params)
        logging.info("Successfully connected to database")

        # Get all temp tables
        temp_tables = get_temp_tables(conn)
        logging.info(f"Found {len(temp_tables)} temp tables")

        # Check each temp table for missing IDs
        results = {}
        for temp_table in temp_tables:
            logging.info(f"Checking table: {temp_table}")
            missing_ids = get_missing_ids(conn, temp_table)
            if missing_ids:
                results[temp_table] = missing_ids
                logging.info(f"Found {len(missing_ids)} missing IDs in {temp_table}")
            else:
                logging.info(f"No missing IDs found in {temp_table}")

        # Print results
        if results:
            logging.info("\nSummary of missing IDs:")
            for table, ids in results.items():
                logging.info(f"\n{table}:")
                for id in sorted(ids):
                    logging.info(f"  {id}")
        else:
            logging.info("No missing IDs found in any temp tables")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info("Database connection closed")

if __name__ == "__main__":
    main() 