# CSV Compare Tool

A high-performance tool for comparing large CSV files and identifying differences. Supports both regular CSV files and compressed files (.gz, .zip).

## Features

- Fast comparison of large CSV files
- Support for compressed files (.gz, .zip)
- Configurable column comparison
- Detailed output of changes
- Progress reporting
- Memory-efficient processing

## Installation

No installation required. Just make sure you have Python 3.6+ installed.

## Usage

Basic usage:
```bash
python csv_compare_hash.py old_file.csv new_file.csv primary_key_column
```

### Command Line Arguments

- `old_file`: Path to the old CSV file (required)
- `new_file`: Path to the new CSV file (required)
- `primary_key`: Column name to use as the primary key (required)
- `--output-dir`: Directory to save output files (default: "comparison_results")
- `--ignore-columns`: Columns to ignore when comparing rows (comma-separated or space-separated)
- `--columns-to-hash`: Only include these columns when comparing (space-separated)
- `--keys-only`: Only store keys in output files, not full rows (saves memory)

### Examples

1. Basic comparison:
```bash
python csv_compare_hash.py old_data.csv new_data.csv id
```

2. Compare compressed files:
```bash
python csv_compare_hash.py old_data.csv.gz new_data.zip id
```

3. Ignore specific columns (comma-separated):
```bash
python csv_compare_hash.py old_data.csv new_data.csv id --ignore-columns "timestamp,updated_at,created_at"
```

4. Ignore specific columns (space-separated):
```bash
python csv_compare_hash.py old_data.csv new_data.csv id --ignore-columns timestamp updated_at created_at
```

5. Only compare specific columns:
```bash
python csv_compare_hash.py old_data.csv new_data.csv id --columns-to-hash name email phone
```

6. Save only keys in output files:
```bash
python csv_compare_hash.py old_data.csv new_data.csv id --keys-only
```

7. Specify custom output directory:
```bash
python csv_compare_hash.py old_data.csv new_data.csv id --output-dir my_comparison_results
```

### Output Files

The tool generates the following files in the output directory:

- `modified_records.csv`: Records that have changed
- `new_records.csv`: Records that exist only in the new file
- `deleted_records.csv`: Records that exist only in the old file
- `comparison_summary.txt`: Summary of the comparison results

### Notes

- The primary key column is automatically ignored when computing row hashes
- When using `--ignore-columns`, you can specify columns either comma-separated or space-separated
- The tool automatically handles compressed files (.gz, .zip)
- For very large files, consider using the `--keys-only` option to reduce memory usage

## License

MIT License

