# CSV Compare Tool

A high-performance tool for comparing CSV files and generating delta reports. The tool efficiently identifies new rows and changes between two CSV files using a specified primary key for row matching.

## Features

- Fast parallel processing of large CSV files
- Support for both plain CSV and gzipped CSV files
- Flexible primary key-based row matching
- Detailed change tracking with JSON output
- Configurable column ignoring
- CPU and memory profiling support
- Auto-detection of latest CSV files in a directory

## Installation

```bash
go install github.com/yourusername/csv-compare@latest
```

## Usage

```bash
csv-compare [flags]
```

### Required Flags

- `--primary-key`: Column name to use as primary key for matching rows (required)

### Optional Flags

- `--prev`: Specific previous CSV file to use (instead of auto-detecting)
- `--curr`: Specific current CSV file to use (instead of auto-detecting)
- `--dir`: Directory path containing CSV files (.csv or .csv.gz) (default: ".")
- `--delta`: Path for the delta CSV output (default: delta_TIMESTAMP.csv)
- `--log`: Path for the changes log JSON output (default: changes_TIMESTAMP.json)
- `--parallel`: Use parallel processing for better performance (default: true)
- `--chunk-size`: Chunk size for parallel processing (default: 1000)
- `--ignore-columns`: Comma-separated list of column names to ignore when determining differences
- `--cpuprofile`: Write CPU profile to file
- `--memprofile`: Write memory profile to file

## How It Works

The tool compares two CSV files using the following logic:

1. **New Rows**: Rows with primary keys that only exist in the current file are considered new and included in the delta output.
2. **Changed Rows**: Rows with the same primary key but different values (excluding ignored columns) are included in both the delta output and changes log.
3. **Identical Rows**: Rows with the same primary key and values are ignored.
4. **Removed Rows**: Rows with primary keys that only exist in the previous file are ignored.

## Examples

### Basic Usage

```bash
# Compare two specific files using 'id' as the primary key
csv-compare --primary-key "id" --prev previous.csv --curr current.csv
```

### Auto-detect Latest Files

```bash
# Compare the two most recent CSV files in a directory
csv-compare --primary-key "id" --dir /path/to/csv/files
```

### Ignore Specific Columns

```bash
# Compare files while ignoring 'last_modified' and 'updated_at' columns
csv-compare --primary-key "id" --prev previous.csv --curr current.csv --ignore-columns "last_modified,updated_at"
```

### Performance Tuning

```bash
# Use a larger chunk size for better performance with large files
csv-compare --primary-key "id" --prev previous.csv --curr current.csv --chunk-size 5000
```

### Profiling

```bash
# Generate CPU and memory profiles
csv-compare --primary-key "id" --prev previous.csv --curr current.csv --cpuprofile cpu.prof --memprofile mem.prof
```

## Output Files

### Delta CSV

The delta CSV file contains:
- All new rows from the current file
- All changed rows from the current file
- Headers from the original files

### Changes Log (JSON)

The changes log contains detailed information about changed rows:
- Row index
- Column name
- Previous value
- Current value

Example JSON output:
```json
[
  {
    "row_index": 42,
    "changes": {
      "name": {
        "column": "name",
        "previous_value": "John Doe",
        "current_value": "John Smith"
      },
      "email": {
        "column": "email",
        "previous_value": "john@example.com",
        "current_value": "john.smith@example.com"
      }
    }
  }
]
```

## Performance Considerations

- The tool uses parallel processing by default for better performance
- Adjust the chunk size based on your file sizes and available memory
- For very large files, consider using gzipped CSV files to reduce I/O
- Use the profiling flags to identify performance bottlenecks

## License

MIT License

