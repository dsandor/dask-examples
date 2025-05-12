# File Size Order Script

## Overview
`file_size_order.py` is a Python script that reads a JSON file containing file metadata and outputs the top N files based on row count in descending order. The script sorts the files by their `rowCount` field and writes the results to a new JSON file.

## Features
- Reads metadata from a JSON file
- Sorts files by row count in descending order
- Outputs the top N files to a JSON file
- Handles errors gracefully (file not found, invalid JSON, etc.)
- Supports command-line arguments for customization

## Usage
```bash
# Use default settings (reads metadata.json, outputs top 10 files)
python file_size_order.py

# Specify a different metadata file
python file_size_order.py path/to/metadata.json

# Specify a different metadata file and number of top results
python file_size_order.py path/to/metadata.json 5
```

## Input Format
The script expects a JSON file with an array of objects, each containing at least a `rowCount` field:

```json
[
  {
    "filename": "BEST_AMER_OUT",
    "relativePath": "BEST_AMER_OUT/best_amer.out.20250504.csv.gz",
    "effectiveDate": "20250504",
    "rowCount": 622401
  },
  ...
]
```

## Output
The script creates a file named `file_size_order.json` containing the top N files sorted by row count in descending order. The output maintains the original structure of each file's metadata.

## Implementation Details
The script:
1. Reads the metadata JSON file
2. Sorts the data by the `rowCount` field in descending order
3. Takes the top N entries
4. Writes the result to `file_size_order.json`

## Error Handling
The script handles the following error cases:
- File not found
- Invalid JSON format
- Invalid command-line arguments

## Dependencies
- Python 3.x
- Standard library modules: `json`, `sys`

## Example
With the sample metadata.json file containing records for different regions, running the script will produce a file_size_order.json with the regions sorted by their row counts, with the largest first (EURO, ASIA, AMER, AFRICA, OCEANIA).
