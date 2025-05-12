# CSV Compare Tool (Python Implementation)

A high-performance tool for comparing large gzipped CSV files and generating delta reports. This Python implementation uses efficient chunked processing, making it significantly faster than the Go implementation for extremely large files.

## Features

- **Optimized for Extremely Large Files**: Processes data in chunks without loading everything into memory
- **Memory Efficient**: Manages memory usage through chunked processing
- **Performance Options**: Includes option to unzip files first for faster processing
- **Support for Gzipped Files**: Works with both plain CSV and gzipped CSV files
- **Primary Key-Based Comparison**: Matches rows across files using a specified primary key
- **Detailed Change Tracking**: Generates a JSON file with all changes
- **Summary Statistics**: Provides detailed statistics about the comparison

## Requirements

- Python 3.7+
- Pandas
- NumPy

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/csv-compare.git
cd csv-compare
```

2. Install the required packages:
```bash
pip install -r requirements.txt
```

## Usage

```bash
python csv_compare.py [flags]
```

### Required Flags

- `--primary-key`: Column name to use as primary key for matching rows (required)

### Optional Flags

- `--prev`: Specific previous CSV file to use (instead of auto-detecting)
- `--curr`: Specific current CSV file to use (instead of auto-detecting)
- `--dir`: Directory path containing CSV files (.csv or .csv.gz) (default: ".")
- `--delta`: Path for the delta CSV output (default: delta_TIMESTAMP.csv)
- `--log`: Path for the changes log JSON output (default: changes_TIMESTAMP.json)
- `--ignore-columns`: Comma-separated list of column names to ignore when determining differences
- `--chunk-size`: Chunk size for processing in number of rows per chunk
- `--unzip`: Unzip files before processing for better performance

## How It Works

The Python implementation efficiently processes large CSV files:

1. **Chunked Processing**: Files are read in chunks, allowing processing of files larger than available RAM
2. **Dictionary-Based Comparison**: Uses dictionaries for fast lookups instead of expensive index operations
3. **Optional Pre-Unzipping**: Can unzip files before processing for significantly faster performance
4. **Efficient Comparison**: 
   - New rows: Rows with primary keys that only exist in the current file
   - Changed rows: Rows with the same primary key but different values (excluding ignored columns)
   - Identical rows: Rows with the same primary key and values (not included in output)
   - Removed rows: Rows with primary keys that only exist in the previous file (not included in output)

## Output

### Delta CSV

The delta CSV file contains:
- All new rows from the current file
- All changed rows from the current file
- Headers from the original files

### Changes Log (JSON)

The changes log contains detailed information about changed rows:
```json
{
  "primary_key_value": {
    "column_name": {
      "column": "column_name",
      "previous_value": "old_value",
      "current_value": "new_value"
    },
    "another_column": {
      "column": "another_column",
      "previous_value": "old_value",
      "current_value": "new_value"
    }
  }
}
```

### Stats File (JSON)

The stats file contains summary information:
```json
{
  "current_row_count": 1000000,
  "delta_row_count": 15000,
  "changed_columns": ["column1", "column2", "column3"],
  "new_records": 10000,
  "removed_records": 5000
}
```

## Examples

### Basic Usage

```bash
# Compare two specific files using 'id' as the primary key
python csv_compare.py --primary-key "id" --prev previous.csv.gz --curr current.csv.gz
```

### Auto-detect Latest Files

```bash
# Compare the two most recent CSV files in a directory
python csv_compare.py --primary-key "id" --dir /path/to/csv/files
```

### Ignore Specific Columns

```bash
# Compare files while ignoring 'last_modified' and 'updated_at' columns
python csv_compare.py --primary-key "id" --prev previous.csv.gz --curr current.csv.gz --ignore-columns "last_modified,updated_at"
```

### Performance Tuning

```bash
# Use a specific chunk size for better performance with large files
python csv_compare.py --primary-key "id" --prev previous.csv.gz --curr current.csv.gz --chunk-size 100000

# Unzip files before processing for significantly faster performance
python csv_compare.py --primary-key "id" --prev previous.csv.gz --curr current.csv.gz --unzip

# Combine chunk size and unzip options for optimal performance with extremely large files
python csv_compare.py --primary-key "id" --prev previous.csv.gz --curr current.csv.gz --chunk-size 100000 --unzip
```

## Performance Comparison with Go Implementation

The Python implementation offers several advantages over the Go implementation for extremely large files:

1. **Memory Efficiency**: Processes data in chunks, allowing it to handle files larger than available RAM
2. **Optimized Dictionary Lookups**: Uses Python's highly optimized dictionary operations for fast comparisons
3. **Unzip Option**: Provides the option to unzip files before processing for significantly faster performance
4. **Flexible Chunking**: Allows tuning the chunk size based on available memory and file characteristics

## Performance Tips

1. **Use the Unzip Option**: For extremely large files, using the `--unzip` flag can improve performance by 30-50% or more, especially on I/O-bound systems
2. **Adjust Chunk Size**: Experiment with different chunk sizes to find the optimal balance between memory usage and processing speed
3. **Disk Space Considerations**: When using the `--unzip` option, ensure you have enough disk space for the uncompressed versions of your files
4. **Memory Management**: The `--chunk-size` parameter (in rows) lets you control memory usage - start with 100,000 and adjust based on your system
5. **Repeated Operations**: If you're comparing the same files multiple times, using the `--unzip` option once and keeping the uncompressed files can save significant time

## License

MIT License
