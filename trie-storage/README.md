# Trie Storage

A Go application that processes gzipped CSV files and stores them in a trie-based directory structure.

## Features

- Reads gzipped CSV files from a source directory
- Processes the most recent file from each subdirectory
- Stores data in a trie-based directory structure
- Optional property history tracking with data lineage
- Supports different destination roots for asset and company files
- Colorful console output for better visibility
- Configurable metadata and analysis file locations

## Requirements

- Go 1.21 or later

## Usage

```bash
go run . -source /path/to/source \
         -asset-dest /path/to/asset/destination \
         -company-dest /path/to/company/destination \
         -metadata /path/to/metadata.json \
         -analysis /path/to/analysis.json \
         -history
```

### Command Line Arguments

- `-source`: Root path containing the source CSV files (required)
- `-asset-dest`: Destination root path for asset files (required)
- `-company-dest`: Destination root path for company files (required)
- `-metadata`: Path to metadata.json file (default: "metadata.json")
- `-analysis`: Path to analysis.json file (default: "analysis.json")
- `-history`: Enable history tracking (default: false)

## Data Structure

The application creates a trie-based directory structure where:
- Each character in the ID_BB_GLOBAL becomes a subdirectory
- The leaf directory contains a JSON file with the asset data
- A history.json file tracks property changes and their sources (when enabled)

## File Format

### Asset Data JSON
```json
{
  "ID_BB_GLOBAL": "string",
  "properties": {
    "property1": "value1",
    "property2": "value2"
  }
}
```

### History JSON
```json
{
  "propertyName": {
    "YYYYMMDD": {
      "file": "filename.csv.gz",
      "value": "value"
    }
  }
}
```

## Console Output

The application provides colorful console output to help track progress and identify important information:

- Info messages: Cyan
- Success messages: Green
- Warning messages: Yellow
- Error messages: Red
- Debug messages: Magenta
- File paths: Bright Blue
- IDs: Bright Green
- Dates: Bright Yellow
- Values: Bright White (Bold)
