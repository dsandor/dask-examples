# CSV File summarize

A high-performance Python script for processing large gzipped CSV files across multiple directories. The script is designed to efficiently process large datasets while maintaining low memory usage.

## Features

- Processes gzipped CSV files from multiple directories
- Automatically selects the most recent file from each directory based on date in filename (YYYYMMDD format)
- Counts total rows across all processed files
- Collects and deduplicates ID_BB_GLOBAL values
- Outputs a CSV file containing all unique ID_BB_GLOBAL values
- Supports directory filtering using include/exclude regex patterns
- Memory-efficient processing of large files
- Progress reporting and statistics

## Requirements

- Python 3.6 or higher
- pandas library

Install required packages:
```bash
pip install pandas
```

## Usage

```bash
python process_csv_files.py --import-root DIR [--include-regex PATTERN] [--exclude-regex PATTERN] --output OUTPUT_CSV
```

### Arguments

- `--import-root`: (Required) Root directory to search for CSV files
- `--include-regex`: (Optional) Regex pattern to match directory names to include
- `--exclude-regex`: (Optional) Regex pattern to match directory names to exclude
- `--output`: (Required) Output CSV file path for unique ID_BB_GLOBAL values
- `--help`: Show help message

### Examples

1. Process all directories under /data/feeds:
```bash
python process_csv_files.py --import-root /data/feeds --output unique_ids.csv
```

2. Process directories matching a specific pattern:
```bash
python process_csv_files.py --import-root /data/feeds --include-regex "company_.*" --output unique_ids.csv
```

3. Process directories matching one pattern but excluding another:
```bash
python process_csv_files.py --import-root /data/feeds --include-regex "finance_.*" --exclude-regex ".*_test" --output unique_ids.csv
```

## File Structure Requirements

- Each directory should contain one or more gzipped CSV files (`.csv.gz` extension)
- CSV files should contain a column named `ID_BB_GLOBAL`
- Filenames should include a date in YYYYMMDD format (e.g., `data_20240315.csv.gz`)
- The script will automatically select the most recent file from each directory based on the date in the filename

## Performance Optimizations

The script includes several optimizations for processing large files:

1. **Memory Efficiency**:
   - Only reads the required `ID_BB_GLOBAL` column
   - Uses sets for efficient unique value tracking
   - Processes one file at a time
   - Uses memory mapping for file reading

2. **Processing Speed**:
   - Uses pandas' C engine for faster CSV parsing
   - Specifies data types to avoid mixed type inference
   - Efficient date-based file selection
   - Optimized unique value collection

## Output

The script provides the following output:

1. **Console Output**:
   - Directory search results
   - Processing progress for each file
   - Row counts per file
   - Unique ID counts per file
   - Total row count across all files
   - Total unique ID count

2. **CSV File**:
   - Creates a CSV file containing all unique ID_BB_GLOBAL values
   - Values are sorted alphabetically
   - Single column named 'ID_BB_GLOBAL'

## Error Handling

The script handles various error conditions:

- Invalid directories are reported and skipped
- Directories without CSV files are reported and skipped
- Files without valid dates in their names are skipped
- Invalid CSV files are reported and skipped
- Missing ID_BB_GLOBAL column is handled gracefully

## Memory Usage

The script is designed to be memory-efficient:
- Memory usage is proportional to the number of unique ID_BB_GLOBAL values
- File processing is done one at a time
- Only necessary columns are loaded into memory
- Memory mapping is used for efficient file reading

## Contributing

Feel free to submit issues and enhancement requests! 