# csv-compare

A high-performance Go application that finds the latest and previous CSV files in a directory and compares them. Changes are written to a delta CSV file. Column differences are logged to a JSON file indicating which column was different along with the previous and current values.

## Features

- Automatically detects the latest and previous CSV files in a directory
- Supports both regular CSV files and gzipped CSV files (.csv.gz)
- Uses dates in filenames (YYYYMMDD format) to determine the latest and previous files
- Generates a delta CSV file containing only the changed records
- Creates a detailed JSON log of all column-level changes
- Ability to ignore specific columns when determining differences
- Supports both sequential and parallel processing for optimal performance
- Includes performance profiling capabilities
- Colorful console output for better readability
- Optimized for speed with large CSV files

## Usage

```bash
# Basic usage with automatic file detection
go run cmd/csv-compare/main.go -dir /path/to/csv/files

# Specify custom output paths
go run cmd/csv-compare/main.go -dir /path/to/csv/files -delta custom_delta.csv -log custom_changes.json

# Use specific CSV files instead of auto-detection
go run cmd/csv-compare/main.go -prev previous.csv -curr current.csv

# Disable parallel processing
go run cmd/csv-compare/main.go -parallel=false

# Adjust chunk size for parallel processing
go run cmd/csv-compare/main.go -chunk-size 5000

# Ignore specific columns when determining differences
go run cmd/csv-compare/main.go -ignore-columns="timestamp,updated_at,version"

# Enable CPU profiling
go run cmd/csv-compare/main.go -cpuprofile cpu.prof

# Enable memory profiling
go run cmd/csv-compare/main.go -memprofile mem.prof
```

## Performance Optimization

The application is optimized for speed using several techniques:

1. Parallel processing with configurable worker count and chunk size
2. Efficient in-memory data structures for fast lookups
3. Minimized I/O operations
4. Built-in profiling for performance analysis and tuning

## Building

```bash
go build -o csv-compare cmd/csv-compare/main.go
```

## Requirements

- Go 1.18 or higher

