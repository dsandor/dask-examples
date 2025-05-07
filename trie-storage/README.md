# Trie Storage

A Go application that processes gzipped CSV files and stores them in a trie-based directory structure.

## Features

- Reads gzipped CSV files from a source directory
- Processes the most recent file from each subdirectory
- Stores data in a trie-based directory structure
- Maintains property history with data lineage
- Supports different destination roots for asset and company files

## Requirements

- Go 1.21 or later

## Usage

```bash
go run . -source /path/to/source -asset-dest /path/to/asset/destination -company-dest /path/to/company/destination
```

### Command Line Arguments

- `-source`: Root path containing the source CSV files
- `-asset-dest`: Destination root path for asset files
- `-company-dest`: Destination root path for company files

## Data Structure

The application creates a trie-based directory structure where:
- Each character in the ID_BB_GLOBAL becomes a subdirectory
- The leaf directory contains a JSON file with the asset data
- A history.json file tracks property changes and their sources

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
  "propertyName": "string",
  "history": [
    {
      "value": "value",
      "sourceFile": "filename.csv.gz",
      "effectiveDate": "YYYYMMDD"
    }
  ]
}
```
