# CSV Compare Tool (Python Implementation)

A high-performance tool for comparing large gzipped CSV files and generating delta reports. This Python implementation uses Dask for parallel processing, making it significantly faster than the Go implementation for extremely large files.

## Features

- **Optimized for Extremely Large Files**: Uses Dask to process data in chunks without loading everything into memory
- **Parallel Processing**: Automatically distributes work across CPU cores
- **Memory Efficient**: Processes data in chunks to minimize memory usage
- **Support for Gzipped Files**: Works with both plain CSV and gzipped CSV files
- **Primary Key-Based Comparison**: Matches rows across files using a specified primary key
- **Detailed Change Tracking**: Generates a JSON file with all changes
- **Summary Statistics**: Provides detailed statistics about the comparison

## Requirements

- Python 3.7+
- Dask
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
- `--chunk-size`: Chunk size for processing in bytes (default: auto)

## How It Works

The Python implementation uses Dask, a parallel computing library, to efficiently process large CSV files:

1. **Chunked Processing**: Files are read in chunks, allowing processing of files larger than available RAM
2. **Parallel Computation**: Operations are automatically parallelized across available CPU cores
3. **Efficient Comparison**: 
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
python csv_compare.py --primary-key "id" --prev previous.csv.gz --curr current.csv.gz --chunk-size 100000000
```

## Performance Comparison with Go Implementation

The Python implementation offers several advantages over the Go implementation for extremely large files:

1. **Memory Efficiency**: Dask processes data in chunks, allowing it to handle files larger than available RAM
2. **Automatic Parallelization**: Dask automatically optimizes parallel execution based on available resources
3. **Lazy Evaluation**: Operations are only performed when needed, reducing unnecessary computation
4. **Optimized Data Structures**: Uses specialized data structures designed for large-scale data processing

## Performance Tips

1. **Adjust Chunk Size**: For very large files, experiment with different chunk sizes to find the optimal balance between memory usage and processing speed
2. **Use Gzipped Files**: Working with gzipped CSV files reduces I/O overhead, which can be a significant bottleneck
3. **Available Memory**: Ensure your system has enough memory for the chunk size you specify
4. **CPU Cores**: The implementation automatically uses all available CPU cores, but performance scales with more cores

## License

MIT License
