#!/usr/bin/env python3
"""
CSV Compare Tool - Fast implementation for comparing large gzipped CSV files
"""
import os
import sys
import json
import time
import argparse
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path
import gzip


class CSVCompare:
    """
    A high-performance tool for comparing CSV files and generating delta reports.
    Uses chunked processing for memory efficiency with large files.
    """
    def __init__(self, prev_file, curr_file, primary_key, ignore_columns=None, chunk_size=None, unzip_files=False):
        """
        Initialize the CSV comparison tool.
        
        Args:
            prev_file (str): Path to the previous CSV file (can be gzipped)
            curr_file (str): Path to the current CSV file (can be gzipped)
            primary_key (str): Column name to use as primary key for matching rows
            ignore_columns (list): List of column names to ignore when determining differences
            chunk_size (int): Chunk size for Dask processing (None for auto)
        """
        self.prev_file = prev_file
        self.curr_file = curr_file
        self.primary_key = primary_key
        self.ignore_columns = ignore_columns or []
        # Always ignore ROWNUMBER and FILEDATE columns
        if 'ROWNUMBER' not in self.ignore_columns:
            self.ignore_columns.append('ROWNUMBER')
        if 'FILEDATE' not in self.ignore_columns:
            self.ignore_columns.append('FILEDATE')
        self.chunk_size = chunk_size
        self.unzip_files = unzip_files
        self.temp_files = []  # Track temporary unzipped files
        
        # Add default columns to ignore
        self.stats = {
            'current_row_count': 0,
            'delta_row_count': 0,
            'changed_columns': set(),
            'new_records': 0,
            'removed_records': 0
        }
    
    def unzip_file(self, file_path):
        """
        Unzip a gzipped file to a temporary location for faster processing.
        
        Args:
            file_path (str): Path to the gzipped file
            
        Returns:
            str: Path to the unzipped temporary file
        """
        if not file_path.endswith('.gz'):
            return file_path
        
        print(f"Unzipping file: {file_path}")
        temp_file = file_path[:-3] + ".temp.csv"
        
        with gzip.open(file_path, 'rb') as f_in:
            with open(temp_file, 'wb') as f_out:
                f_out.write(f_in.read())
        
        self.temp_files.append(temp_file)
        return temp_file
    
    def read_csv_file(self, file_path):
        """
        Read a CSV file (gzipped or not) into a pandas DataFrame.
        For large files, this uses chunked reading to manage memory usage.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            pandas.DataFrame: The loaded DataFrame
        """
        # Unzip file if requested
        if self.unzip_files and file_path.endswith('.gz'):
            file_path = self.unzip_file(file_path)
            compression = None
        else:
            compression = 'gzip' if file_path.endswith('.gz') else None
        
        print(f"Reading file: {file_path}")
        
        # For large files, read in chunks and process
        if self.chunk_size:
            chunks = []
            chunksize = self.chunk_size
            for chunk in pd.read_csv(file_path, compression=compression, chunksize=chunksize):
                # Ensure primary key is a string to avoid type comparison issues
                chunk[self.primary_key] = chunk[self.primary_key].astype(str)
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True)
        else:
            # For smaller files, read all at once
            df = pd.read_csv(file_path, compression=compression)
            # Ensure primary key is a string to avoid type comparison issues
            df[self.primary_key] = df[self.primary_key].astype(str)
        
        return df
    
    def compare(self, delta_csv_path=None, changes_log_path=None):
        """
        Compare the two CSV files and generate delta and changes log.
        
        Args:
            delta_csv_path (str): Path to save the delta CSV file
            changes_log_path (str): Path to save the changes log JSON file
            
        Returns:
            tuple: (delta_df, changes_dict) - The delta DataFrame and changes dictionary
        """
        start_time = time.time()
        print(f"Starting comparison between files...")
        
        # Set default output paths if not provided
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if not delta_csv_path:
            delta_csv_path = f"delta_{timestamp}.csv"
        if not changes_log_path:
            changes_log_path = f"changes_{timestamp}.json"
        
        # Read both files
        prev_df = self.read_csv_file(self.prev_file)
        curr_df = self.read_csv_file(self.curr_file)
        
        # Get all columns except ignored ones
        all_columns = list(curr_df.columns)
        compare_columns = [col for col in all_columns if col not in self.ignore_columns]
        
        # Store current row count
        self.stats['current_row_count'] = len(curr_df)
        
        # Create dictionaries for faster lookups
        print("Creating lookup dictionaries...")
        prev_dict = {}
        for _, row in prev_df.iterrows():
            key = str(row[self.primary_key])
            prev_dict[key] = row.to_dict()
        
        # Process current file and compare with previous
        print("Comparing files...")
        changes = {}
        changed_keys = []
        new_keys = []
        
        # Create a set of keys from the previous file for faster lookups
        prev_keys_set = set(prev_dict.keys())
        
        # Process current file
        for _, row in curr_df.iterrows():
            key = str(row[self.primary_key])
            
            # Check if key exists in previous file
            if key in prev_dict:
                # Compare values for this key
                row_changes = {}
                curr_row_dict = row.to_dict()
                prev_row_dict = prev_dict[key]
                
                for col in compare_columns:
                    if col in prev_row_dict and col in curr_row_dict:
                        # Handle NaN values properly
                        prev_val = prev_row_dict[col]
                        curr_val = curr_row_dict[col]
                        
                        # Convert to string for comparison if not NaN
                        if not pd.isna(prev_val) and not pd.isna(curr_val):
                            prev_val = str(prev_val)
                            curr_val = str(curr_val)
                        
                        # Compare values
                        if not pd.isna(prev_val) and not pd.isna(curr_val) and prev_val != curr_val:
                            row_changes[col] = {
                                "column": col,
                                "previous_value": prev_val,
                                "current_value": curr_val
                            }
                            # Track columns with differences
                            self.stats['changed_columns'].add(col)
                
                # If changes were found, add to the changes dictionary
                if row_changes:
                    changes[key] = row_changes
                    changed_keys.append(key)
            else:
                # Key doesn't exist in previous file, so it's a new record
                new_keys.append(key)
        
        # Update stats
        self.stats['new_records'] = len(new_keys)
        
        # Find removed keys (in previous but not in current)
        curr_keys_set = set(curr_df[self.primary_key].astype(str))
        removed_keys = list(prev_keys_set - curr_keys_set)
        self.stats['removed_records'] = len(removed_keys)
        
        # Create delta dataframe with new and changed records
        delta_df = curr_df[curr_df[self.primary_key].astype(str).isin(new_keys + changed_keys)]
        
        # Update stats
        self.stats['delta_row_count'] = len(delta_df)
        self.stats['changed_columns'] = list(self.stats['changed_columns'])
        
        # Write delta to CSV
        print(f"Writing delta file to {delta_csv_path}")
        if delta_csv_path.endswith('.gz'):
            delta_df.to_csv(delta_csv_path, index=False, compression='gzip')
        else:
            delta_df.to_csv(delta_csv_path, index=False)
        
        # Write changes to JSON
        print(f"Writing changes log to {changes_log_path}")
        with open(changes_log_path, 'w') as f:
            json.dump(changes, f, indent=2)
        
        # Write stats to JSON
        stats_path = os.path.splitext(delta_csv_path)[0] + "_stats.json"
        print(f"Writing stats to {stats_path}")
        
        # Create a separate file for columns with differences
        columns_path = os.path.splitext(delta_csv_path)[0] + "_changed_columns.txt"
        print(f"Writing changed columns to {columns_path}")
        with open(columns_path, 'w') as f:
            f.write("Columns with differences:\n")
            for col in sorted(self.stats['changed_columns']):
                f.write(f"{col}\n")
        
        # Convert set to list for JSON serialization in stats file
        stats_for_json = self.stats.copy()
        stats_for_json['changed_columns'] = sorted(list(self.stats['changed_columns']))
        
        with open(stats_path, 'w') as f:
            json.dump(stats_for_json, f, indent=2)
        
        # Clean up temporary files
        self.cleanup_temp_files()
        
        elapsed_time = time.time() - start_time
        print(f"Comparison completed in {elapsed_time:.2f} seconds")
        print(f"Summary:")
        print(f"  Current file row count: {self.stats['current_row_count']}")
        print(f"  Delta file row count: {self.stats['delta_row_count']}")
        print(f"  New records: {self.stats['new_records']}")
        print(f"  Changed records: {len(changed_keys)}")
        print(f"  Removed records: {self.stats['removed_records']}")
        
        if self.stats['changed_columns']:
            print("\nColumns with differences:")
            for col in sorted(self.stats['changed_columns']):
                print(f"  - {col}")
            print(f"\nDetailed list of changed columns written to: {os.path.basename(columns_path)}")
        
        return delta_df, changes
        
    def cleanup_temp_files(self):
        """
        Clean up any temporary files created during processing.
        """
        for temp_file in self.temp_files:
            try:
                if os.path.exists(temp_file):
                    print(f"Removing temporary file: {temp_file}")
                    os.remove(temp_file)
            except Exception as e:
                print(f"Warning: Could not remove temporary file {temp_file}: {e}")


def find_latest_csv_files(directory='.'):
    """
    Find the two most recent CSV files in a directory.
    
    Args:
        directory (str): Directory to search for CSV files
        
    Returns:
        tuple: (prev_file, curr_file) - Paths to the previous and current CSV files
    """
    csv_files = []
    for file in os.listdir(directory):
        if file.endswith('.csv') or file.endswith('.csv.gz'):
            full_path = os.path.join(directory, file)
            csv_files.append((full_path, os.path.getmtime(full_path)))
    
    # Sort by modification time (newest last)
    csv_files.sort(key=lambda x: x[1])
    
    if len(csv_files) < 2:
        raise ValueError(f"Need at least 2 CSV files in directory {directory}")
    
    # Return the two most recent files
    return csv_files[-2][0], csv_files[-1][0]


def main():
    """Main entry point for the CSV comparison tool."""
    parser = argparse.ArgumentParser(description='Compare two CSV files and generate delta report')
    parser.add_argument('--primary-key', required=True, help='Column name to use as primary key')
    parser.add_argument('--prev', help='Previous CSV file path')
    parser.add_argument('--curr', help='Current CSV file path')
    parser.add_argument('--dir', default='.', help='Directory containing CSV files')
    parser.add_argument('--delta', help='Path for delta CSV output')
    parser.add_argument('--log', help='Path for changes log JSON output')
    parser.add_argument('--ignore-columns', help='Comma-separated list of columns to ignore')
    parser.add_argument('--chunk-size', type=int, help='Chunk size for processing (number of rows per chunk)')
    parser.add_argument('--unzip', action='store_true', help='Unzip files before processing for better performance')
    
    args = parser.parse_args()
    
    # Get file paths
    if args.prev and args.curr:
        prev_file = args.prev
        curr_file = args.curr
    else:
        prev_file, curr_file = find_latest_csv_files(args.dir)
    
    # Parse ignored columns
    ignore_columns = []
    if args.ignore_columns:
        ignore_columns = args.ignore_columns.split(',')
    
    # Create timestamp for output files if not specified
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    delta_path = args.delta if args.delta else f"delta_{timestamp}.csv"
    log_path = args.log if args.log else f"changes_{timestamp}.json"
    
    # Create and run the comparison
    comparator = CSVCompare(
        prev_file=prev_file,
        curr_file=curr_file,
        primary_key=args.primary_key,
        ignore_columns=ignore_columns,
        chunk_size=args.chunk_size,
        unzip_files=args.unzip
    )
    
    comparator.compare(delta_path, log_path)


if __name__ == "__main__":
    main()
