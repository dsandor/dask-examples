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

Additionally, it can analyze the metadata to identify files with specific columns and categorize them.

## Installation

```bash
go get github.com/dsandor/meta-extract
```

## Usage

The application provides two commands:

### Extract Command

Extracts metadata from files in a directory structure.

```bash
# Run with default settings (current directory, output to metadata.json)
go run cmd/meta-extract/main.go extract

# Specify a custom root path and output file
go run cmd/meta-extract/main.go extract -path /path/to/data -output custom-metadata.json
```

### Analyze Command

Analyzes a metadata JSON file to identify files with specific columns and categorize them.

```bash
# Run with default settings (reads metadata.json, outputs to analysis.json)
go run cmd/meta-extract/main.go analyze

# Specify custom input and output files
go run cmd/meta-extract/main.go analyze -input metadata.json -output custom-analysis.json
```

The analyze command:
- Identifies files with ID_BB_GLOBAL column (categorized as "asset files")
- Identifies files with ID_BB_COMPANY but no ID_BB_GLOBAL column (categorized as "company files")
- Logs exceptions (files with neither ID_BB_GLOBAL nor ID_BB_COMPANY)

### Building the Binary

```bash
# Build the binary
go build -o meta-extract ./cmd/meta-extract

# Run the binary
./meta-extract extract -path /path/to/data
./meta-extract analyze -input metadata.json
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

### Metadata JSON (Extract Command)

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

### Analysis JSON (Analyze Command)

```json
{
  "assetFiles": [
    "products",
    "securities"
  ],
  "companyFiles": [
    "customers",
    "companies"
  ],
  "exceptions": [
    "orders"
  ]
}
