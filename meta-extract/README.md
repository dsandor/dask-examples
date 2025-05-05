# meta-extract

The purpose of this application is to extract column metadata from csv files in a directory structure.

## Overview

This Go module scans a directory structure for gzipped CSV files, extracts metadata from each file, and outputs the metadata in JSON format. The metadata includes:

- Filename
- Relative file path
- Effective date (extracted from the filename in YYYYMMDD format)
- Column names
- Number of rows
- PostgreSQL compatible data type for each column

## Installation

```bash
go get github.com/dsandor/meta-extract
```

## Usage

### Command Line

```bash
# Run with default settings (current directory, output to metadata.json)
go run cmd/meta-extract/main.go

# Specify a custom root path and output file
go run cmd/meta-extract/main.go -path /path/to/data -output custom-metadata.json
```

### Building the Binary

```bash
# Build the binary
go build -o meta-extract ./cmd/meta-extract

# Run the binary
./meta-extract -path /path/to/data
```

## Expected Directory Structure

The module expects a directory structure where:
- Each directory name is the name of the file
- Files are gzipped CSV files
- Filenames contain a date in YYYYMMDD format

Example:
```
/root
  /customers
    customers_20240101.gz
  /products
    products_20240215.gz
  /orders
    orders_20240320.gz
```

## Output Format

The metadata is output as a JSON file with the following structure:

```json
[
  {
    "filename": "customers",
    "relativePath": "customers/customers_20240101.gz",
    "effectiveDate": "20240101",
    "rowCount": 3,
    "columns": [
      {
        "name": "id",
        "dataType": "integer"
      },
      {
        "name": "name",
        "dataType": "text"
      },
      ...
    ]
  },
  ...
]
```
