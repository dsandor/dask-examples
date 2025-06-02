#!/usr/bin/env python3

import pandas as pd
import argparse
from pathlib import Path
from colorama import init, Fore, Style
from typing import Tuple

# Initialize colorama
init()

def format_number(num: int) -> str:
    """Format number with commas for better readability."""
    return f"{num:,}"

def print_colored(text: str, color: str = Fore.WHITE, style: str = Style.NORMAL) -> None:
    """Print colored text with the specified color and style."""
    print(f"{style}{color}{text}{Style.RESET_ALL}")

def analyze_column(input_file: str, output_file: str, column_name: str, min_count: int = 1) -> Tuple[int, int, int]:
    """
    Analyze a CSV file and count unique values in the specified column.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path where the results will be saved
        column_name (str): Name of the column to analyze
        min_count (int): Minimum count threshold for values to be included in output
        
    Returns:
        Tuple[int, int, int]: Total rows, unique values, and filtered values counts
    """
    # Read the CSV file with optimized settings
    df = pd.read_csv(
        input_file,
        usecols=[column_name],  # Only read the column we need
        dtype_backend='pyarrow',  # Use pyarrow for better performance
        engine='pyarrow'  # Use pyarrow engine for faster CSV reading
    )
    
    total_rows = len(df)
    value_counts = df[column_name].value_counts()
    unique_values = len(value_counts)
    
    # Filter values based on min_count
    filtered_counts = value_counts[value_counts >= min_count]
    filtered_values = len(filtered_counts)
    
    # Convert to DataFrame and save to CSV
    result_df = pd.DataFrame({
        'value': filtered_counts.index,
        'count': filtered_counts.values
    })
    
    result_df.to_csv(output_file, index=False)
    
    return total_rows, unique_values, filtered_values

def print_summary(total_rows: int, unique_values: int, filtered_values: int, min_count: int) -> None:
    """Print a colorized summary of the analysis results."""
    print_colored("\n=== Analysis Summary ===", Fore.CYAN, Style.BRIGHT)
    print_colored(f"Total Rows Processed: {format_number(total_rows)}", Fore.GREEN)
    print_colored(f"Unique Values Found: {format_number(unique_values)}", Fore.YELLOW)
    print_colored(f"Values with Count ≥ {min_count}: {format_number(filtered_values)}", Fore.MAGENTA)
    
    if min_count > 1:
        excluded = unique_values - filtered_values
        print_colored(f"Values Excluded (Count < {min_count}): {format_number(excluded)}", Fore.RED)

def main():
    parser = argparse.ArgumentParser(description='Analyze unique values in a CSV column')
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('output_file', help='Path to the output CSV file')
    parser.add_argument('column_name', help='Name of the column to analyze')
    parser.add_argument('--min-count', type=int, default=1,
                      help='Minimum count threshold for values to be included in output (default: 1)')
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not Path(args.input_file).exists():
        print_colored(f"Error: Input file '{args.input_file}' does not exist", Fore.RED, Style.BRIGHT)
        return
    
    try:
        print_colored(f"\nAnalyzing column '{args.column_name}' in {args.input_file}...", Fore.CYAN)
        total_rows, unique_values, filtered_values = analyze_column(
            args.input_file, args.output_file, args.column_name, args.min_count
        )
        
        print_summary(total_rows, unique_values, filtered_values, args.min_count)
        print_colored(f"\nResults saved to: {args.output_file}", Fore.GREEN)
        
    except Exception as e:
        print_colored(f"Error: {str(e)}", Fore.RED, Style.BRIGHT)

if __name__ == '__main__':
    main() 