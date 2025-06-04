#!/usr/bin/env python3
"""
CSV File Processor

This script processes CSV files from matching directories, counting rows and unique ID_BB_GLOBAL values.
It uses optimized libraries for handling large CSV files efficiently.

Usage:
    python process_csv_files.py --import-root DIR [--include-regex PATTERN] [--exclude-regex PATTERN] --output OUTPUT_CSV

Arguments:
    --import-root       - Root directory to search for directories
    --include-regex     - Regex pattern to match directory names to include
    --exclude-regex     - Regex pattern to match directory names to exclude
    --output           - Output CSV file path for unique ID_BB_GLOBAL values
    --help             - Show this help message
"""

import os
import re
import sys
import argparse
import gzip
import glob
from typing import List, Optional, Set
from datetime import datetime
import pandas as pd
from pathlib import Path

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
            
        include_match = True if include_regex is None else bool(include_regex.search(item))
        exclude_match = False if exclude_regex is None else bool(exclude_regex.search(item))
        
        if include_match and not exclude_match:
            matching_dirs.append(item_path)
            
    return sorted(matching_dirs)

def get_latest_csv_gz(directory: str) -> Optional[str]:
    """
    Find the most recent CSV gzip file in the directory based on the date in the filename.
    The date should be in YYYYMMDD format within the filename.
    """
    gz_files = glob.glob(os.path.join(directory, "*.csv.gz"))
    if not gz_files:
        return None
        
    # Extract dates from filenames and find the most recent
    latest_file = None
    latest_date = None
    
    for file_path in gz_files:
        # Extract date from filename using regex
        match = re.search(r'(\d{8})', os.path.basename(file_path))
        if match:
            date_str = match.group(1)
            try:
                file_date = datetime.strptime(date_str, '%Y%m%d')
                if latest_date is None or file_date > latest_date:
                    latest_date = file_date
                    latest_file = file_path
            except ValueError:
                continue
    
    return latest_file

def process_csv_file(gz_path: str) -> tuple[int, Set[str]]:
    """
    Process a gzipped CSV file, counting rows and collecting unique ID_BB_GLOBAL values.
    
    Args:
        gz_path: Path to the gzipped CSV file
        
    Returns:
        Tuple of (row_count, set of unique ID_BB_GLOBAL values)
    """
    # Use pandas with optimized settings for large files
    df = pd.read_csv(
        gz_path,
        compression='gzip',
        usecols=['ID_BB_GLOBAL'],  # Only read the column we need
        dtype={'ID_BB_GLOBAL': str},  # Specify dtype to avoid mixed types
        engine='c',  # Use C engine for better performance
        memory_map=True  # Memory map the file for better performance
    )
    
    return len(df), set(df['ID_BB_GLOBAL'].dropna().unique())

def main():
    parser = argparse.ArgumentParser(
        description="Process CSV files from matching directories, counting rows and unique ID_BB_GLOBAL values.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all directories under /data/feeds
  python process_csv_files.py --import-root /data/feeds --output unique_ids.csv
  
  # Process directories matching a pattern
  python process_csv_files.py --import-root /data/feeds --include-regex "company_.*" --output unique_ids.csv
""")
    parser.add_argument("--import-root", required=True, help="Root directory to search for directories")
    parser.add_argument("--include-regex", help="Regex pattern to match directory names to include")
    parser.add_argument("--exclude-regex", help="Regex pattern to match directory names to exclude")
    parser.add_argument("--output", required=True, help="Output CSV file path for unique ID_BB_GLOBAL values")
    
    args = parser.parse_args()
    
    print(f"Searching for directories in {args.import_root}")
    print(f"Include pattern: {args.include_regex or 'None (include all)'}")
    print(f"Exclude pattern: {args.exclude_regex or 'None (exclude none)'}")
    
    matching_dirs = find_matching_directories(
        args.import_root, 
        args.include_regex, 
        args.exclude_regex
    )
    
    print(f"\nFound {len(matching_dirs)} matching directories")
    
    total_rows = 0
    all_unique_ids = set()
    
    for dir_path in matching_dirs:
        gz_path = get_latest_csv_gz(dir_path)
        if not gz_path:
            print(f"No CSV gzip files found in {dir_path}")
            continue
            
        print(f"\nProcessing {gz_path}")
        row_count, unique_ids = process_csv_file(gz_path)
        total_rows += row_count
        all_unique_ids.update(unique_ids)
        
        print(f"Rows in file: {row_count:,}")
        print(f"Unique ID_BB_GLOBAL values in file: {len(unique_ids):,}")
    
    print(f"\nTotal rows across all files: {total_rows:,}")
    print(f"Total unique ID_BB_GLOBAL values: {len(all_unique_ids):,}")
    
    # Save unique IDs to CSV
    output_df = pd.DataFrame({'ID_BB_GLOBAL': sorted(all_unique_ids)})
    output_df.to_csv(args.output, index=False)
    print(f"\nSaved unique ID_BB_GLOBAL values to {args.output}")

if __name__ == "__main__":
    main() 