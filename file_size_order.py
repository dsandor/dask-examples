#!/usr/bin/env python3
import json
import sys

def get_top_files_by_row_count(metadata_file, top_n=10):
    """
    Read a metadata JSON file and return the top N files based on rowCount in descending order.
    
    Args:
        metadata_file (str): Path to the metadata JSON file
        top_n (int): Number of top files to return
        
    Returns:
        list: List of dictionaries containing the top N files
    """
    try:
        # Read the metadata file
        with open(metadata_file, 'r') as f:
            data = json.load(f)
        
        # Sort the data by rowCount in descending order
        sorted_data = sorted(data, key=lambda x: x.get('rowCount', 0), reverse=True)
        
        # Return the top N files
        return sorted_data[:top_n]
    except FileNotFoundError:
        print(f"Error: File '{metadata_file}' not found.")
        return []
    except json.JSONDecodeError:
        print(f"Error: '{metadata_file}' is not a valid JSON file.")
        return []
    except Exception as e:
        print(f"Error: {str(e)}")
        return []

def main():
    # Default values
    metadata_file = 'metadata.json'
    top_n = 10
    
    # Parse command line arguments
    if len(sys.argv) > 1:
        metadata_file = sys.argv[1]
    if len(sys.argv) > 2:
        try:
            top_n = int(sys.argv[2])
        except ValueError:
            print(f"Error: '{sys.argv[2]}' is not a valid number for top_n.")
            return
    
    # Get the top files
    top_files = get_top_files_by_row_count(metadata_file, top_n)
    
    # Write the result to file_size_order.json
    if top_files:
        try:
            with open('file_size_order.json', 'w') as f:
                json.dump(top_files, f, indent=2)
            print(f"Successfully wrote top {len(top_files)} files to file_size_order.json")
        except Exception as e:
            print(f"Error writing to file_size_order.json: {str(e)}")

if __name__ == "__main__":
    main()
