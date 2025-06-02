#!/usr/bin/env python3

import pandas as pd
import argparse
from pathlib import Path

def analyze_column(input_file: str, output_file: str, column_name: str) -> None:
    """
    Analyze a CSV file and count unique values in the specified column.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to the output CSV file
        column_name (str): Name of the column to analyze
    """
    # Read the CSV file with optimized settings
    df = pd.read_csv(
        input_file,
        usecols=[column_name],  # Only read the column we need
        dtype_backend='pyarrow',  # Use pyarrow for better performance
        engine='pyarrow'  # Use pyarrow engine for faster CSV reading
    )
    
    # Count unique values
    value_counts = df[column_name].value_counts()
    
    # Convert to DataFrame and save to CSV
    result_df = pd.DataFrame({
        'value': value_counts.index,
        'count': value_counts.values
    })
    
    result_df.to_csv(output_file, index=False)
    print(f"Analysis complete. Results saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Analyze unique values in a CSV column')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('output_file', help='Path to the output CSV file')
    parser.add_argument('column_name', help='Name of the column to analyze')
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not Path(args.input_file).exists():
        print(f"Error: Input file '{args.input_file}' does not exist")
        return
    
    try:
        analyze_column(args.input_file, args.output_file, args.column_name)
    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == '__main__':
    main() 