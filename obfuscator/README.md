# CSV Obfuscator

A Python utility for obfuscating columns in large CSV files efficiently.

## Features

- Handles gigabyte-sized CSV files
- Multiple obfuscation methods: hashing, random replacement, or masking
- Process only a subset of rows if needed
- Progress reporting for large files
- Customizable CSV delimiter and quote character

## Requirements

- Python 3.6+

## Usage

```bash
python csv_obfuscator.py input.csv output.csv --columns col1,col2 [--rows N] [--method hash|random|mask]
```

### Arguments

- `input.csv` - Input CSV file path
- `output.csv` - Output CSV file path
- `--columns` - Comma-separated list of column names to obfuscate
- `--rows` - Optional: Number of rows to process (default: all)
- `--method` - Optional: Obfuscation method (hash, random, mask) (default: hash)
- `--delimiter` - Optional: CSV delimiter (default: ',')
- `--quotechar` - Optional: CSV quote character (default: '"')
- `--help` - Show help message

### Obfuscation Methods

1. **hash** - Replaces values with their SHA-256 hash (default)
2. **random** - Replaces values with random characters of the same length
3. **mask** - Keeps first and last characters, replaces middle with asterisks

## Examples

Obfuscate email and phone columns in a CSV file:
```bash
python csv_obfuscator.py data.csv obfuscated_data.csv --columns email,phone
```

Process only the first 1000 rows:
```bash
python csv_obfuscator.py data.csv obfuscated_data.csv --columns email,phone --rows 1000
```

Use masking instead of hashing:
```bash
python csv_obfuscator.py data.csv obfuscated_data.csv --columns email,phone --method mask
```

Use a different delimiter:
```bash
python csv_obfuscator.py data.csv obfuscated_data.csv --columns email,phone --delimiter ";"
```

## Performance

The script is designed to handle large files efficiently:
- Processes files line by line to minimize memory usage
- Reports progress periodically
- Can be stopped after processing a specific number of rows
