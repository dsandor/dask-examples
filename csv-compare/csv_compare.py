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
import dask.dataframe as dd
import pandas as pd
import numpy as np


class CSVCompare:
    """
    A high-performance tool for comparing CSV files and generating delta reports.
    Uses Dask for parallel processing of large files.
    """
    def __init__(self, prev_file, curr_file, primary_key, ignore_columns=None, chunk_size=None):
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
        
        # Add default columns to ignore
        self.stats = {
            'current_row_count': 0,
            'delta_row_count': 0,
            'changed_columns': set(),
            'new_records': 0,
            'removed_records': 0
        }
    
    def read_csv_file(self, file_path):
        """
        Read a CSV file (gzipped or not) into a Dask DataFrame.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            dask.DataFrame: The loaded DataFrame
        """
        print(f"Reading file: {file_path}")
        compression = 'gzip' if file_path.endswith('.gz') else None
        
        # Use Dask to read the file in chunks
        if self.chunk_size:
            df = dd.read_csv(file_path, compression=compression, blocksize=self.chunk_size)
        else:
            df = dd.read_csv(file_path, compression=compression)
            
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
        
        # Set primary key as index for faster joins
        prev_df = prev_df.set_index(self.primary_key)
        curr_df = curr_df.set_index(self.primary_key)
        
        # Store current row count
        self.stats['current_row_count'] = len(curr_df)
        
        # Find records in current but not in previous (new records)
        # and records in both but with changes
        
        # First, identify keys in both dataframes
        prev_keys = prev_df.index.compute()
        curr_keys = curr_df.index.compute()
        
        # Find new keys (in current but not in previous)
        new_keys = list(set(curr_keys) - set(prev_keys))
        self.stats['new_records'] = len(new_keys)
        
        # Find removed keys (in previous but not in current)
        removed_keys = list(set(prev_keys) - set(curr_keys))
        self.stats['removed_records'] = len(removed_keys)
        
        # Find common keys (in both)
        common_keys = list(set(curr_keys) & set(prev_keys))
        
        # Extract records with common keys
        prev_common = prev_df.loc[common_keys]
        curr_common = curr_df.loc[common_keys]
        
        # Compare only the columns we care about
        changes = {}
        changed_keys = []
        
        # Process in chunks to avoid memory issues
        chunk_size = 10000  # Adjust based on available memory
        for i in range(0, len(common_keys), chunk_size):
            chunk_keys = common_keys[i:i+chunk_size]
            
            # Get chunks from both dataframes
            prev_chunk = prev_df.loc[chunk_keys].compute()
            curr_chunk = curr_df.loc[chunk_keys].compute()
            
            # Compare each row
            for key in chunk_keys:
                if key not in prev_chunk.index or key not in curr_chunk.index:
                    continue
                    
                prev_row = prev_chunk.loc[key]
                curr_row = curr_chunk.loc[key]
                
                # Check for differences in compare columns
                row_changes = {}
                for col in compare_columns:
                    if col in prev_row and col in curr_row:
                        # Handle NaN values properly
                        prev_val = prev_row[col]
                        curr_val = curr_row[col]
                        
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
                            self.stats['changed_columns'].add(col)
                
                # If changes were found, add to the changes dictionary
                if row_changes:
                    changes[key] = row_changes
                    changed_keys.append(key)
        
        # Create delta dataframe with new and changed records
        delta_keys = new_keys + changed_keys
        delta_df = curr_df.loc[delta_keys].compute()
        
        # Reset index to include primary key as a column
        delta_df = delta_df.reset_index()
        
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
        with open(stats_path, 'w') as f:
            # Convert set to list for JSON serialization
            json.dump(self.stats, f, indent=2)
        
        elapsed_time = time.time() - start_time
        print(f"Comparison completed in {elapsed_time:.2f} seconds")
        print(f"Summary:")
        print(f"  Current file row count: {self.stats['current_row_count']}")
        print(f"  Delta file row count: {self.stats['delta_row_count']}")
        print(f"  New records: {self.stats['new_records']}")
        print(f"  Changed records: {len(changed_keys)}")
        print(f"  Removed records: {self.stats['removed_records']}")
        
        if self.stats['changed_columns']:
            print("Columns with differences:")
            for col in self.stats['changed_columns']:
                print(f"  - {col}")
        
        return delta_df, changes


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
    parser.add_argument('--chunk-size', type=int, help='Chunk size for processing (in bytes)')
    
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
        chunk_size=args.chunk_size
    )
    
    comparator.compare(delta_path, log_path)


if __name__ == "__main__":
    main()
