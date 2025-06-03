#!/usr/bin/env python3
"""
Directory Lister

This script lists directories under a root directory that match include/exclude patterns.
It uses the same directory matching logic as csv_to_postgres.py.

Usage:
    python list_directories.py --import-root DIR [--include-regex PATTERN] [--exclude-regex PATTERN]

Arguments:
    --import-root       - Root directory to search for directories
    --include-regex     - Regex pattern to match directory names to include
    --exclude-regex     - Regex pattern to match directory names to exclude
    --help              - Show this help message
"""

import os
import re
import sys
import argparse
from typing import List, Optional

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
            
        # Check if directory matches include pattern
        include_match = True if include_regex is None else bool(include_regex.search(item))
        
        # Check if directory matches exclude pattern
        exclude_match = False if exclude_regex is None else bool(exclude_regex.search(item))
        
        if include_match and not exclude_match:
            matching_dirs.append(item_path)
            
    return sorted(matching_dirs)

def main():
    parser = argparse.ArgumentParser(
        description="List directories under a root directory that match include/exclude patterns.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all directories under /data/feeds
  python list_directories.py --import-root /data/feeds
  
  # List directories matching a pattern
  python list_directories.py --import-root /data/feeds --include-regex "company_.*"
  
  # List directories matching one pattern but not another
  python list_directories.py --import-root /data/feeds --include-regex "finance_.*" --exclude-regex ".*_test"
""")
    parser.add_argument("--import-root", required=True, help="Root directory to search for directories")
    parser.add_argument("--include-regex", help="Regex pattern to match directory names to include")
    parser.add_argument("--exclude-regex", help="Regex pattern to match directory names to exclude")
    
    args = parser.parse_args()
    
    print(f"Searching for directories in {args.import_root}")
    print(f"Include pattern: {args.include_regex or 'None (include all)'}")
    print(f"Exclude pattern: {args.exclude_regex or 'None (exclude none)'}")
    
    matching_dirs = find_matching_directories(
        args.import_root, 
        args.include_regex, 
        args.exclude_regex
    )
    
    print(f"\nFound {len(matching_dirs)} matching directories:")
    for i, dir_path in enumerate(matching_dirs, 1):
        print(f"{i}. {dir_path}")

if __name__ == "__main__":
    main() 