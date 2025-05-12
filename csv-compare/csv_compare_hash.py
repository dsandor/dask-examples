#!/usr/bin/env python3
import csv
import os
import time
import argparse
import hashlib
import json
import logging
import gzip
import zipfile
import tempfile
from collections import defaultdict

def setup_logging():
    """Configure logging for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )
    return logging.getLogger(__name__)

def decompress_file(filepath):
    """
    Decompress a file if it's compressed (.zip or .gz).
    Returns the path to the decompressed file or the original file if not compressed.
    """
    logger = logging.getLogger(__name__)
    
    if filepath.endswith('.gz'):
        logger.info(f"Decompressing gzip file: {filepath}")
        with gzip.open(filepath, 'rt', encoding='utf-8') as f_in:
            # Create a temporary file to store the decompressed content
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
            with open(temp_file.name, 'w', encoding='utf-8') as f_out:
                f_out.write(f_in.read())
            return temp_file.name
            
    elif filepath.endswith('.zip'):
        logger.info(f"Decompressing zip file: {filepath}")
        with zipfile.ZipFile(filepath, 'r') as zip_ref:
            # Create a temporary directory
            temp_dir = tempfile.mkdtemp()
            # Extract all contents
            zip_ref.extractall(temp_dir)
            # Get the first CSV file from the zip
            csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
            if not csv_files:
                raise ValueError(f"No CSV files found in zip archive: {filepath}")
            return os.path.join(temp_dir, csv_files[0])
            
    return filepath

def create_row_hash(row, columns_to_hash, ignore_columns):
    """
    Create a hash for a row based on specified columns.
    
    Args:
        row: Dictionary representing a CSV row
        columns_to_hash: List of column names to include in hash (None means all)
        ignore_columns: List of column names to exclude from hash
    
    Returns:
        String hash representing the row content
    """
    # Determine which columns to include in the hash
    if columns_to_hash:
        hash_data = {k: row[k] for k in columns_to_hash if k in row}
    else:
        hash_data = {k: v for k, v in row.items() if k not in ignore_columns}
    
    # Create a stable string representation and hash it
    hash_string = json.dumps(hash_data, sort_keys=True)
    return hashlib.md5(hash_string.encode()).hexdigest()

def process_csv_to_dict(filepath, primary_key, columns_to_hash=None, ignore_columns=None, 
                        progress_interval=500000):
    """
    Process a CSV file and create a dictionary mapping primary keys to row hashes.
    
    Args:
        filepath: Path to the CSV file
        primary_key: Column name to use as the primary key
        columns_to_hash: List of column names to include in hash (None means all except ignored)
        ignore_columns: List of column names to exclude from hash
        progress_interval: How often to log progress (in rows)
    
    Returns:
        Dictionary mapping primary keys to row hashes
    """
    logger = logging.getLogger(__name__)
    
    if ignore_columns is None:
        ignore_columns = []
    
    # Always ignore the primary key when computing the hash
    if primary_key not in ignore_columns:
        ignore_columns.append(primary_key)
    
    key_to_hash = {}
    key_to_row = {}
    row_count = 0
    
    logger.info(f"Processing file: {filepath}")
    start_time = time.time()
    
    with open(filepath, 'r', newline='', encoding='utf-8') as csvfile:
        # Detect CSV dialect
        sample = csvfile.read(4096)
        csvfile.seek(0)
        dialect = csv.Sniffer().sniff(sample)
        
        # Process the CSV file
        reader = csv.DictReader(csvfile, dialect=dialect)
        
        # Verify the primary key exists
        if primary_key not in reader.fieldnames:
            raise ValueError(f"Primary key '{primary_key}' not found in CSV columns: {reader.fieldnames}")
        
        for row in reader:
            row_count += 1
            
            # Get the primary key value
            key = row[primary_key]
            if not key:  # Skip rows with empty primary keys
                continue
            
            # Create a hash for the row data (excluding ignored columns)
            row_hash = create_row_hash(row, columns_to_hash, ignore_columns)
            
            # Store the hash and optionally the full row
            key_to_hash[key] = row_hash
            key_to_row[key] = row
            
            # Log progress periodically
            if row_count % progress_interval == 0:
                elapsed = time.time() - start_time
                logger.info(f"Processed {row_count:,} rows ({row_count/elapsed:.0f} rows/sec)")
    
    elapsed_time = time.time() - start_time
    logger.info(f"Completed processing {row_count:,} rows in {elapsed_time:.2f} seconds")
    logger.info(f"Found {len(key_to_hash):,} unique keys")
    
    return key_to_hash, key_to_row

def write_csv(filepath, fieldnames, rows):
    """Write rows to a CSV file."""
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def compare_csvs(old_file, new_file, primary_key, output_dir, 
                columns_to_hash=None, ignore_columns=None, 
                keep_full_data=True, sample_size=10000):
    """
    Compare two large CSV files and identify differences and new records.
    
    Args:
        old_file: Path to the old CSV file
        new_file: Path to the new CSV file
        primary_key: Column name to use as the primary key
        output_dir: Directory to save output files
        columns_to_hash: List of column names to include in hash (None means all)
        ignore_columns: List of column names to ignore when comparing
        keep_full_data: Whether to keep full row data for output
        sample_size: Number of rows to sample for headers
    
    Returns:
        Tuple containing counts of modified, new, and deleted records
    """
    logger = logging.getLogger(__name__)
    
    if ignore_columns is None:
        ignore_columns = []
    
    start_time = time.time()
    logger.info(f"Starting comparison of {old_file} and {new_file}")
    logger.info(f"Using primary key: {primary_key}")
    if columns_to_hash:
        logger.info(f"Including only these columns in comparison: {columns_to_hash}")
    if ignore_columns:
        logger.info(f"Ignoring columns: {ignore_columns}")
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Decompress files if needed
    old_file = decompress_file(old_file)
    new_file = decompress_file(new_file)
    
    # Output file paths
    modified_output_path = os.path.join(output_dir, "modified_records.csv")
    new_output_path = os.path.join(output_dir, "new_records.csv")
    deleted_output_path = os.path.join(output_dir, "deleted_records.csv")
    summary_output_path = os.path.join(output_dir, "comparison_summary.txt")
    
    # Phase 1: Build dictionaries for both files
    logger.info("Phase 1: Building dictionaries for both files")
    
    # Process old file
    old_key_to_hash, old_key_to_row = process_csv_to_dict(
        old_file, primary_key, columns_to_hash, ignore_columns
    )
    
    # Process new file
    new_key_to_hash, new_key_to_row = process_csv_to_dict(
        new_file, primary_key, columns_to_hash, ignore_columns
    )
    
    # Phase 2: Compare dictionaries to find differences
    logger.info("Phase 2: Comparing dictionaries to identify differences")
    
    # Get sample fieldnames from files for output
    # Get fieldnames from the actual files
    with open(new_file, 'r', newline='', encoding='utf-8') as f:
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample)
        new_fieldnames = csv.DictReader(f, dialect=dialect).fieldnames
    
    with open(old_file, 'r', newline='', encoding='utf-8') as f:
        sample = f.read(4096)
        f.seek(0)
        dialect = csv.Sniffer().sniff(sample)
        old_fieldnames = csv.DictReader(f, dialect=dialect).fieldnames
    
    # Find modified, new, and deleted records
    modified_keys = []
    new_keys = []
    deleted_keys = []
    
    # Find modified and new records
    for key, new_hash in new_key_to_hash.items():
        if key in old_key_to_hash:
            if old_key_to_hash[key] != new_hash:
                modified_keys.append(key)
        else:
            new_keys.append(key)
    
    # Find deleted records
    for key in old_key_to_hash:
        if key not in new_key_to_hash:
            deleted_keys.append(key)
    
    logger.info(f"Found {len(modified_keys):,} modified records")
    logger.info(f"Found {len(new_keys):,} new records")
    logger.info(f"Found {len(deleted_keys):,} deleted records")
    
    # Phase 3: Write results to files
    logger.info("Phase 3: Writing results to output files")
    
    # Write modified records
    if keep_full_data and modified_keys:
        modified_rows = [new_key_to_row[key] for key in modified_keys if key in new_key_to_row]
        write_csv(modified_output_path, new_fieldnames, modified_rows)
    elif modified_keys:
        with open(modified_output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(modified_keys))
    
    # Write new records
    if keep_full_data and new_keys:
        new_rows = [new_key_to_row[key] for key in new_keys if key in new_key_to_row]
        write_csv(new_output_path, new_fieldnames, new_rows)
    elif new_keys:
        with open(new_output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(new_keys))
    
    # Write deleted records
    if keep_full_data and deleted_keys:
        deleted_rows = [old_key_to_row[key] for key in deleted_keys if key in old_key_to_row]
        write_csv(deleted_output_path, old_fieldnames, deleted_rows)
    elif deleted_keys:
        with open(deleted_output_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(deleted_keys))
    
    elapsed_time = time.time() - start_time
    
    # Generate summary
    with open(summary_output_path, 'w') as summary_file:
        summary_file.write("CSV Comparison Summary\n")
        summary_file.write("====================\n\n")
        summary_file.write(f"Old file: {os.path.basename(old_file)}\n")
        summary_file.write(f"New file: {os.path.basename(new_file)}\n")
        summary_file.write(f"Primary key: {primary_key}\n\n")
        
        if ignore_columns:
            summary_file.write(f"Ignored columns: {', '.join(ignore_columns)}\n")
        if columns_to_hash:
            summary_file.write(f"Compared columns: {', '.join(columns_to_hash)}\n")
        
        summary_file.write(f"\nModified records: {len(modified_keys):,}\n")
        summary_file.write(f"New records: {len(new_keys):,}\n")
        summary_file.write(f"Deleted records: {len(deleted_keys):,}\n")
        summary_file.write(f"\nProcessing time: {elapsed_time:.2f} seconds\n")
    
    logger.info(f"Comparison completed in {elapsed_time:.2f} seconds")
    logger.info(f"Results saved to {output_dir}")
    
    return len(modified_keys), len(new_keys), len(deleted_keys)

def main():
    parser = argparse.ArgumentParser(description="Compare two large CSV files efficiently")
    parser.add_argument("old_file", help="Path to the old CSV file (supports .csv, .csv.gz, or .zip)")
    parser.add_argument("new_file", help="Path to the new CSV file (supports .csv, .csv.gz, or .zip)")
    parser.add_argument("primary_key", help="Column name to use as the primary key")
    parser.add_argument("--output-dir", default="comparison_results", 
                        help="Directory to save output files (default: comparison_results)")
    parser.add_argument("--ignore-columns", nargs='+', default=[],
                        help="Columns to ignore when comparing rows")
    parser.add_argument("--columns-to-hash", nargs='+', default=None,
                        help="Only include these columns when comparing (default: all except ignored)")
    parser.add_argument("--keys-only", action="store_true", 
                        help="Only store keys in output files, not full rows (saves memory)")
    
    args = parser.parse_args()
    
    # Set up logging
    logger = setup_logging()
    
    try:
        compare_csvs(
            args.old_file, 
            args.new_file, 
            args.primary_key,
            args.output_dir,
            args.columns_to_hash,
            args.ignore_columns,
            not args.keys_only
        )
    except Exception as e:
        logger.error(f"Error during comparison: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())