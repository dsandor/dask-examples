#!/usr/bin/env python3

import pandas as pd
import argparse
from pathlib import Path
from colorama import init, Fore, Style
from typing import List
import glob

# Initialize colorama
init()

def format_number(num: int) -> str:
    """Format number with commas for better readability."""
    return f"{num:,}"

def print_colored(text: str, color: str = Fore.WHITE, style: str = Style.NORMAL) -> None:
    """Print colored text with the specified color and style."""
    print(f"{style}{color}{text}{Style.RESET_ALL}")

def read_analysis_file(file_path: str) -> pd.DataFrame:
    """
    Read a single analysis output file.
    
    Args:
        file_path (str): Path to the analysis output file
        
    Returns:
        pd.DataFrame: DataFrame containing the analysis data
    """
    try:
        df = pd.read_csv(file_path)
        if not all(col in df.columns for col in ['value', 'count']):
            raise ValueError(f"Invalid file format in {file_path}. Expected columns: value, count")
        return df
    except Exception as e:
        raise ValueError(f"Error reading {file_path}: {str(e)}")

def merge_analysis_files(input_files: List[str], output_file: str) -> None:
    """
    Merge multiple analysis output files and aggregate their counts.
    
    Args:
        input_files (List[str]): List of input file paths
        output_file (str): Path to save the merged output
    """
    # Read and combine all files
    dfs = []
    total_files = len(input_files)
    
    print_colored(f"\nProcessing {total_files} analysis files...", Fore.CYAN)
    
    for i, file_path in enumerate(input_files, 1):
        print_colored(f"Reading file {i}/{total_files}: {file_path}", Fore.YELLOW)
        df = read_analysis_file(file_path)
        dfs.append(df)
    
    # Combine all DataFrames
    combined_df = pd.concat(dfs, ignore_index=True)
    
    # Group by value and sum the counts
    merged_df = combined_df.groupby('value', as_index=False)['count'].sum()
    
    # Sort by count in descending order
    merged_df = merged_df.sort_values('count', ascending=False)
    
    # Save the merged results
    merged_df.to_csv(output_file, index=False)
    
    # Print summary
    print_colored("\n=== Merge Summary ===", Fore.CYAN, Style.BRIGHT)
    print_colored(f"Total Files Processed: {format_number(total_files)}", Fore.GREEN)
    print_colored(f"Total Unique Values: {format_number(len(merged_df))}", Fore.YELLOW)
    print_colored(f"Total Count: {format_number(merged_df['count'].sum())}", Fore.MAGENTA)
    print_colored(f"\nResults saved to: {output_file}", Fore.GREEN)

def main():
    parser = argparse.ArgumentParser(description='Merge multiple analysis output files and aggregate their counts')
    parser.add_argument('input_files', nargs='+', help='Input files to merge (can be glob patterns or explicit file paths)')
    parser.add_argument('output_file', help='Path to save the merged output')
    
    args = parser.parse_args()
    
    # Process input files
    input_files = []
    for pattern in args.input_files:
        # Check if the pattern is a glob pattern
        if any(c in pattern for c in '*?[]'):
            matched_files = glob.glob(pattern)
            if not matched_files:
                print_colored(f"Warning: No files found matching pattern '{pattern}'", Fore.YELLOW)
            input_files.extend(matched_files)
        else:
            # If not a glob pattern, treat as a direct file path
            if Path(pattern).exists():
                input_files.append(pattern)
            else:
                print_colored(f"Warning: File not found: '{pattern}'", Fore.YELLOW)
    
    # Remove duplicates while preserving order
    input_files = list(dict.fromkeys(input_files))
    
    if not input_files:
        print_colored("Error: No valid input files found", Fore.RED, Style.BRIGHT)
        return
    
    try:
        merge_analysis_files(input_files, args.output_file)
    except Exception as e:
        print_colored(f"Error: {str(e)}", Fore.RED, Style.BRIGHT)

if __name__ == '__main__':
    main() 