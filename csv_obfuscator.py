#!/usr/bin/env python3
"""
CSV Obfuscator - A tool to obfuscate columns in large CSV files

Usage:
    python csv_obfuscator.py input.csv output.csv --columns col1,col2 [--rows N] [--method hash|random|mask]

Arguments:
    input.csv       - Input CSV file path
    output.csv      - Output CSV file path
    --columns       - Comma-separated list of column names to obfuscate
    --rows          - Optional: Number of rows to process (default: all)
    --method        - Optional: Obfuscation method (hash, random, mask) (default: hash)
    --delimiter     - Optional: CSV delimiter (default: ',')
    --quotechar     - Optional: CSV quote character (default: '"')
    --help          - Show this help message

Example:
    python csv_obfuscator.py data.csv obfuscated_data.csv --columns email,phone --rows 1000 --method hash
"""

import sys
import csv
import hashlib
import random
import string
import argparse
from typing import List, Dict, Callable, Optional
import os


def hash_value(value: str) -> str:
    """Hash a value using SHA-256."""
    return hashlib.sha256(value.encode()).hexdigest()


def random_value(value: str) -> str:
    """Replace with random string of same length."""
    if not value:
        return ""
    chars = string.ascii_letters + string.digits
    return ''.join(random.choice(chars) for _ in range(len(value)))


def mask_value(value: str) -> str:
    """Mask value, keeping first and last characters."""
    if len(value) <= 2:
        return value
    return value[0] + '*' * (len(value) - 2) + value[-1]


def get_obfuscation_function(method: str) -> Callable[[str], str]:
    """Return the appropriate obfuscation function based on method."""
    methods = {
        'hash': hash_value,
        'random': random_value,
        'mask': mask_value
    }
    if method not in methods:
        print(f"Warning: Unknown method '{method}', using 'hash' instead.")
        return methods['hash']
    return methods[method]


def obfuscate_csv(
    input_file: str,
    output_file: str,
    columns: List[str],
    max_rows: Optional[int] = None,
    method: str = 'hash',
    delimiter: str = ',',
    quotechar: str = '"'
) -> None:
    """
    Obfuscate specified columns in a CSV file.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
        columns: List of column names to obfuscate
        max_rows: Maximum number of rows to process (None for all)
        method: Obfuscation method (hash, random, mask)
        delimiter: CSV delimiter character
        quotechar: CSV quote character
    """
    obfuscate_func = get_obfuscation_function(method)
    
    try:
        # Get file size for progress reporting
        file_size = os.path.getsize(input_file)
        
        with open(input_file, 'r', newline='') as infile, \
             open(output_file, 'w', newline='') as outfile:
            
            reader = csv.DictReader(infile, delimiter=delimiter, quotechar=quotechar)
            
            # Validate columns exist in the file
            header = reader.fieldnames
            if not header:
                raise ValueError("Input CSV file has no header row")
            
            invalid_columns = [col for col in columns if col not in header]
            if invalid_columns:
                raise ValueError(f"Columns not found in CSV: {', '.join(invalid_columns)}")
            
            writer = csv.DictWriter(outfile, fieldnames=header, delimiter=delimiter, quotechar=quotechar)
            writer.writeheader()
            
            # Process rows
            rows_processed = 0
            bytes_processed = 0
            last_percent = 0
            
            for row in reader:
                # Obfuscate specified columns
                for column in columns:
                    if column in row:
                        row[column] = obfuscate_func(row[column])
                
                writer.writerow(row)
                rows_processed += 1
                
                # Update progress (every 10,000 rows or when percent changes)
                if rows_processed % 10000 == 0:
                    bytes_processed = infile.tell()
                    percent = int(100 * bytes_processed / file_size)
                    if percent > last_percent:
                        print(f"Progress: {percent}% ({rows_processed:,} rows)", file=sys.stderr)
                        last_percent = percent
                
                # Stop if we've reached max_rows
                if max_rows is not None and rows_processed >= max_rows:
                    break
            
            print(f"Completed: {rows_processed:,} rows processed", file=sys.stderr)
            print(f"Output written to: {output_file}", file=sys.stderr)
    
    except Exception as e:
        print(f"Error: {str(e)}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Obfuscate columns in a CSV file")
    parser.add_argument("input_file", help="Input CSV file")
    parser.add_argument("output_file", help="Output CSV file")
    parser.add_argument("--columns", required=True, help="Comma-separated list of columns to obfuscate")
    parser.add_argument("--rows", type=int, help="Number of rows to process (default: all)")
    parser.add_argument("--method", default="hash", choices=["hash", "random", "mask"], 
                        help="Obfuscation method (default: hash)")
    parser.add_argument("--delimiter", default=",", help="CSV delimiter (default: ',')")
    parser.add_argument("--quotechar", default='"', help="CSV quote character (default: '\"')")
    
    if len(sys.argv) == 1 or "--help" in sys.argv:
        print(__doc__)
        sys.exit(0)
    
    args = parser.parse_args()
    
    columns = [col.strip() for col in args.columns.split(",")]
    
    obfuscate_csv(
        args.input_file,
        args.output_file,
        columns,
        args.rows,
        args.method,
        args.delimiter,
        args.quotechar
    )


if __name__ == "__main__":
    main()
