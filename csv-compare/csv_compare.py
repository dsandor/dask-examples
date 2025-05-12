#!/usr/bin/env python3
"""
CSV Compare Tool - Fast implementation for comparing large gzipped CSV files
"""
import os
import sys
import json
import time
import argparse
import warnings
from datetime import datetime
import pandas as pd
import numpy as np
from pathlib import Path
import gzip

# Suppress pandas warnings about mixed types
warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)


class CSVCompare:
    """
    A high-performance tool for comparing CSV files and generating delta reports.
    Uses chunked processing for memory efficiency with large files.
    """
    def __init__(self, prev_file, curr_file, primary_key, ignore_columns=None, chunk_size=None, 
                 unzip_files=False, detailed_timing=True, dtype_handling='auto'):
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
        self.detailed_timing = detailed_timing
        self.timings = {}  # Store timing information for different stages
        self.dtype_handling = dtype_handling  # How to handle mixed data types
        
        # Add default columns to ignore
        self.stats = {
            'current_row_count': 0,
            'delta_row_count': 0,
            'changed_columns': set(),
            'new_records': 0,
            'removed_records': 0
        }
    
    def time_operation(self, operation_name, func, *args, **kwargs):
        """
        Time an operation and store the result.
        
        Args:
            operation_name (str): Name of the operation for reporting
            func (callable): Function to time
            *args, **kwargs: Arguments to pass to the function
            
        Returns:
            The result of the function call
        """
        if not self.detailed_timing:
            return func(*args, **kwargs)
            
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        elapsed = end_time - start_time
        
        self.timings[operation_name] = elapsed
        print(f"{operation_name} completed in {elapsed:.2f} seconds")
        
        return result
    
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
        
        def _do_unzip():
            try:
                # Check if the file exists and is readable
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"File not found: {file_path}")
                
                # Check if file is empty
                if os.path.getsize(file_path) == 0:
                    raise ValueError(f"File is empty: {file_path}")
                
                # Unzip the file in chunks to handle very large files
                with gzip.open(file_path, 'rb') as f_in:
                    with open(temp_file, 'wb') as f_out:
                        chunk_size = 10 * 1024 * 1024  # 10MB chunks
                        while True:
                            chunk = f_in.read(chunk_size)
                            if not chunk:
                                break
                            f_out.write(chunk)
                
                # Verify the unzipped file is not empty
                if os.path.getsize(temp_file) == 0:
                    raise ValueError(f"Unzipped file is empty: {temp_file}")
                
                return temp_file
            except Exception as e:
                print(f"Error unzipping file {file_path}: {str(e)}")
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                raise
        
        result = self.time_operation(f"Unzipping {os.path.basename(file_path)}", _do_unzip)
        self.temp_files.append(temp_file)
        return result
    
    def read_csv_file(self, file_path):
        """
        Read a CSV file (gzipped or not) into a pandas DataFrame.
        For large files, this uses chunked reading to manage memory usage.
        
        Args:
            file_path (str): Path to the CSV file
            
        Returns:
            pandas.DataFrame: The loaded DataFrame
        """
        try:
            # Check if the file exists and is readable
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
            
            # Check if file is empty
            if os.path.getsize(file_path) == 0:
                raise ValueError(f"File is empty: {file_path}")
            
            # Unzip file if requested
            if self.unzip_files and file_path.endswith('.gz'):
                file_path = self.unzip_file(file_path)
                compression = None
            else:
                compression = 'gzip' if file_path.endswith('.gz') else None
            
            print(f"Reading file: {file_path}")
            
            def _read_file():
                try:
                    # Set up read_csv parameters based on dtype handling strategy
                    csv_params = {
                        'filepath_or_buffer': file_path,
                        'compression': compression,
                    }
                    
                    # Configure dtype handling based on strategy
                    if self.dtype_handling == 'text':
                        # Treat all columns as text - much faster, avoids dtype issues
                        csv_params['dtype'] = str
                        csv_params['low_memory'] = True  # Can use low_memory with string dtype
                    elif self.dtype_handling == 'fast':
                        # Fast mode - use low_memory for speed
                        csv_params['low_memory'] = True
                        # Suppress warnings in fast mode
                        warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
                    else:  # 'auto' mode
                        # Use low_memory=False for more accurate type inference
                        csv_params['low_memory'] = False
                    
                    # For large files, read in chunks and process
                    if self.chunk_size:
                        chunks = []
                        csv_params['chunksize'] = self.chunk_size
                        
                        # Read chunks
                        for chunk in pd.read_csv(**csv_params):
                            # Ensure primary key is a string to avoid type comparison issues
                            chunk[self.primary_key] = chunk[self.primary_key].astype(str)
                            chunks.append(chunk)
                            
                        if not chunks:
                            raise ValueError(f"No data was read from file: {file_path}")
                        return pd.concat(chunks, ignore_index=True)
                    else:
                        # For smaller files, read all at once
                        df = pd.read_csv(**csv_params)
                        if df.empty:
                            raise ValueError(f"File contains no data: {file_path}")
                        # Ensure primary key is a string to avoid type comparison issues
                        df[self.primary_key] = df[self.primary_key].astype(str)
                        return df
                except pd.errors.EmptyDataError:
                    raise ValueError(f"File contains no data or has no columns: {file_path}")
                except Exception as e:
                    print(f"Error reading file {file_path}: {str(e)}")
                    raise
            
            return self.time_operation(f"Reading {os.path.basename(file_path)}", _read_file)
        except Exception as e:
            print(f"Fatal error processing file {file_path}: {str(e)}")
            raise
    
    def compare(self, delta_csv_path=None, changes_log_path=None):
        """
        Compare the two CSV files and generate delta and changes log.
        
        Args:
            delta_csv_path (str): Path to save the delta CSV file
            changes_log_path (str): Path to save the changes log JSON file
            
        Returns:
            tuple: (delta_df, changes_dict) - The delta DataFrame and changes dictionary
        """
        overall_start_time = time.time()
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
        
        def _create_lookup_dict():
            # Optimize: Use a dictionary comprehension for better performance
            # Only include columns we care about to save memory
            columns_to_keep = [self.primary_key] + compare_columns
            columns_to_keep = list(set(columns_to_keep))  # Remove duplicates
            
            # Filter DataFrame to only include columns we need
            filtered_prev_df = prev_df[columns_to_keep]
            
            # Create dictionary with primary key as key and row as value
            prev_dict = {}
            for _, row in filtered_prev_df.iterrows():
                key = str(row[self.primary_key])
                prev_dict[key] = row.to_dict()
            return prev_dict
        
        prev_dict = self.time_operation("Creating lookup dictionary", _create_lookup_dict)
        
        # Process current file and compare with previous
        print("Comparing files...")
        
        def _compare_files():
            changes = {}
            changed_keys = []
            new_keys = []
            
            # Optimize: Create a set of keys from the previous file for faster lookups
            prev_keys_set = set(prev_dict.keys())
            
            # Optimize: Filter current DataFrame to only include columns we need
            columns_to_keep = [self.primary_key] + compare_columns
            columns_to_keep = list(set(columns_to_keep))  # Remove duplicates
            filtered_curr_df = curr_df[columns_to_keep]
            
            # Optimize: Convert primary key column to string once for the whole DataFrame
            # This avoids repeated conversions in the loop
            filtered_curr_df[self.primary_key] = filtered_curr_df[self.primary_key].astype(str)
            
            # Process current file in batches for better performance
            batch_size = 10000  # Process 10,000 rows at a time
            total_rows = len(filtered_curr_df)
            
            for start_idx in range(0, total_rows, batch_size):
                end_idx = min(start_idx + batch_size, total_rows)
                
                # Get batch of rows
                batch = filtered_curr_df.iloc[start_idx:end_idx]
                
                # Process each row in the batch
                for _, row in batch.iterrows():
                    key = row[self.primary_key]  # Already converted to string above
                    
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
                                
                                # If using text_only mode, values are already strings
                                if self.dtype_handling != 'text':
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
                
                # Print progress every 100,000 rows
                if end_idx % 100000 == 0 or end_idx == total_rows:
                    print(f"Processed {end_idx}/{total_rows} rows ({end_idx/total_rows*100:.1f}%)")
            
            # Find removed keys (in previous but not in current)
            curr_keys_set = set(filtered_curr_df[self.primary_key])  # Already strings
            removed_keys = list(prev_keys_set - curr_keys_set)
            
            return changes, changed_keys, new_keys, removed_keys
        
        changes, changed_keys, new_keys, removed_keys = self.time_operation("Comparing file contents", _compare_files)
        
        # Update stats
        self.stats['new_records'] = len(new_keys)
        self.stats['removed_records'] = len(removed_keys)
        
        # Create delta dataframe with new and changed records
        def _create_delta_df():
            return curr_df[curr_df[self.primary_key].astype(str).isin(new_keys + changed_keys)]
        
        delta_df = self.time_operation("Creating delta dataframe", _create_delta_df)
        
        # Update stats
        self.stats['delta_row_count'] = len(delta_df)
        self.stats['changed_columns'] = list(self.stats['changed_columns'])
        
        # Write delta to CSV
        print(f"Writing delta file to {delta_csv_path}")
        
        def _write_delta_csv():
            if delta_csv_path.endswith('.gz'):
                delta_df.to_csv(delta_csv_path, index=False, compression='gzip')
            else:
                delta_df.to_csv(delta_csv_path, index=False)
        
        self.time_operation("Writing delta CSV", _write_delta_csv)
        
        # Write changes to JSON
        print(f"Writing changes log to {changes_log_path}")
        
        def _write_changes_json():
            with open(changes_log_path, 'w') as f:
                json.dump(changes, f, indent=2)
        
        self.time_operation("Writing changes log", _write_changes_json)
        
        # Write stats to JSON
        stats_path = os.path.splitext(delta_csv_path)[0] + "_stats.json"
        print(f"Writing stats to {stats_path}")
        
        # Create a separate file for columns with differences
        columns_path = os.path.splitext(delta_csv_path)[0] + "_changed_columns.txt"
        print(f"Writing changed columns to {columns_path}")
        
        def _write_output_files():
            # Write columns file
            with open(columns_path, 'w') as f:
                f.write("Columns with differences:\n")
                for col in sorted(self.stats['changed_columns']):
                    f.write(f"{col}\n")
            
            # Convert set to list for JSON serialization in stats file
            stats_for_json = self.stats.copy()
            stats_for_json['changed_columns'] = sorted(list(self.stats['changed_columns']))
            
            # Add timing information to stats
            if self.detailed_timing:
                stats_for_json['timings'] = self.timings
            
            # Write stats file
            with open(stats_path, 'w') as f:
                json.dump(stats_for_json, f, indent=2)
        
        self.time_operation("Writing output files", _write_output_files)
        
        # Clean up temporary files
        self.cleanup_temp_files()
        
        overall_elapsed_time = time.time() - overall_start_time
        self.timings['total_time'] = overall_elapsed_time
        
        print(f"\nComparison completed in {overall_elapsed_time:.2f} seconds")
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
        
        if self.detailed_timing:
            print("\nDetailed timing information:")
            for operation, duration in sorted(self.timings.items()):
                print(f"  {operation}: {duration:.2f} seconds")
        
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
    parser.add_argument('--no-timing', action='store_true', help='Disable detailed timing information')
    parser.add_argument('--verify', action='store_true', help='Verify files before processing (check if they can be read)')
    parser.add_argument('--no-warnings', action='store_true', help='Suppress pandas warnings about data types')
    parser.add_argument('--text-only', action='store_true', help='Treat all columns as text (faster, avoids dtype issues)')
    parser.add_argument('--fast', action='store_true', help='Use fastest processing mode (less memory checking, may be less safe)')
    
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
    try:
        # Verify files exist and are readable
        if args.verify:
            print("Verifying files...")
            for file_path in [prev_file, curr_file]:
                if not os.path.exists(file_path):
                    raise FileNotFoundError(f"File not found: {file_path}")
                if os.path.getsize(file_path) == 0:
                    raise ValueError(f"File is empty: {file_path}")
                
                # Try to open and read a small part of the file
                if file_path.endswith('.gz'):
                    with gzip.open(file_path, 'rt') as f:
                        header = f.readline()
                        if not header:
                            raise ValueError(f"File appears to be empty or corrupted: {file_path}")
                        print(f"Verified file {file_path} (header: {header[:50]}...)")
                else:
                    with open(file_path, 'r') as f:
                        header = f.readline()
                        if not header:
                            raise ValueError(f"File appears to be empty: {file_path}")
                        print(f"Verified file {file_path} (header: {header[:50]}...)")
        
        # If no-warnings is specified, suppress pandas warnings
        if args.no_warnings:
            warnings.filterwarnings('ignore', category=pd.errors.DtypeWarning)
        
        # Determine dtype handling strategy
        dtype_handling = 'auto'
        if args.text_only:
            dtype_handling = 'text'
        elif args.fast:
            dtype_handling = 'fast'
        
        comparator = CSVCompare(
            prev_file=prev_file,
            curr_file=curr_file,
            primary_key=args.primary_key,
            ignore_columns=ignore_columns,
            chunk_size=args.chunk_size,
            unzip_files=args.unzip,
            detailed_timing=not args.no_timing,
            dtype_handling=dtype_handling
        )
        
        comparator.compare(delta_path, log_path)
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        print("\nFor troubleshooting, try the following:")
        print("1. Check if both files exist and are not empty")
        print("2. Verify that the files are valid CSV files")
        print("3. Use the --verify flag to check files before processing")
        print("4. If using --unzip, check if there's enough disk space for uncompressed files")
        print("5. Try processing without the --unzip flag")
        sys.exit(1)


if __name__ == "__main__":
    main()
