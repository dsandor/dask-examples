# PG Loader

A set of Python utilities for working with large CSV files and PostgreSQL databases, specifically designed for financial data processing with Bloomberg identifiers.

## Features

### CSV Obfuscator
- Handles gigabyte-sized CSV files
- Multiple obfuscation methods: hashing, random replacement, or masking
- Process only a subset of rows if needed
- Progress reporting for large files
- Customizable CSV delimiter and quote character

### CSV to PostgreSQL Loader
- Loads CSV data into PostgreSQL with an optimized schema
- Uses `ID_BB_GLOBAL` as the primary key
- Stores all other columns as a JSONB object for flexibility
- Advanced merging of data when records are updated
- Efficient batch processing for large files
- Automatic index creation for frequently queried fields
- Docker setup for local PostgreSQL instance

## Database Schema

The loader creates a simple but powerful table structure:

```sql
CREATE TABLE csv_data (
    id_bb_global TEXT PRIMARY KEY,
    data JSONB
);
```

### Indexes

For optimal query performance, the following GIN indexes are automatically created on the JSONB column:

1. `idx_csv_data_id_bb_global_company` - For `ID_BB_GLOBAL_COMPANY`
2. `idx_csv_data_id_isin` - For `ID_ISIN`
3. `idx_csv_data_id_cusip` - For `ID_CUSIP`
4. `idx_csv_data_id_sedol1` - For `ID_SEDOL1`
5. `idx_csv_data_id_sedol2` - For `ID_SEDOL2`

These indexes significantly improve query performance when filtering or searching by these common financial identifiers.

### Data Merge Rules

When processing CSV files, the loader follows these merge rules:

1. **New Records**: If `ID_BB_GLOBAL` doesn't exist in the database, a new record is created.

2. **Existing Records**: If `ID_BB_GLOBAL` exists:
   - All fields from the CSV are added to the existing JSONB object
   - If a field already exists in the database but is not in the CSV, it remains unchanged
   - If a field exists in both the database and CSV, the CSV value takes precedence
   - `NULL` values in the CSV do not overwrite existing values

3. **Special Handling**:
   - The `ID_BB_GLOBAL` column is never stored in the JSONB data (it's used as the primary key)
   - All other columns from the CSV are stored in the JSONB object
   - Field names are preserved exactly as they appear in the CSV headers

## Requirements

- Python 3.6+
- Docker and Docker Compose (for PostgreSQL)
- psycopg2 (for PostgreSQL connection)
- PostgreSQL 10+ (for JSONB functionality)

## Installation

Install the required Python package:

```bash
pip install psycopg2-binary
```

## Usage

### Working with Temporary Tables and Merging Data

This section explains how to load CSV data into temporary PostgreSQL tables and then merge it into the main `csv_data` table.

#### 1. Loading CSV Files into Temporary Tables

Use the `csv_to_temp_tables.py` script to load one or more CSV files into temporary tables:

```bash
python csv_to_temp_tables.py --host localhost --port 5432 --database csvdata --username postgres --password Password123 /path/to/your/files/*.csv
```

This script will:
- Create a temporary table for each CSV file with a name like `temp_[filename]_[random_string]`
- Preserve the original column names and data types from the CSV
- Handle large files efficiently

Example with a sample file:
```bash
python csv_to_temp_tables.py equity_fake.csv
```

#### 2. Verifying the Temporary Tables

To see the list of temporary tables created:

```sql
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'pg_temp' 
AND table_name LIKE 'temp_%';
```

To view the structure of a temporary table:

```sql
\d+ pg_temp.temp_equity_fake_abc123  -- Replace with your actual temp table name
```

#### 3. Merging Data into the Main Table

Use the `merge_jsonb_from_temp` function to merge data from a temporary table into the main `csv_data` table:

```sql
SELECT public.merge_jsonb_from_temp(
    'temp_equity_fake_abc123',  -- Replace with your temp table name
    'ID_BB_GLOBAL',             -- ID column name (case-sensitive)
    'csv_data',                 -- Target table
    'data',                     -- JSONB column in target table
    ARRAY['created_at', 'updated_at']  -- Columns to exclude from merge
);
```

#### 4. Verifying the Merge

Check the merged data:

```sql
-- View a sample of merged records
SELECT id_bb_global, jsonb_pretty(data) 
FROM csv_data 
WHERE id_bb_global IN (
    SELECT "ID_BB_GLOBAL" 
    FROM pg_temp.temp_equity_fake_abc123  -- Replace with your temp table name
    LIMIT 5
);

-- Count merged records
SELECT COUNT(*) 
FROM csv_data 
WHERE id_bb_global IN (
    SELECT "ID_BB_GLOBAL" 
    FROM pg_temp.temp_equity_fake_abc123  -- Replace with your temp table name
);
```

### Starting PostgreSQL with Docker

```bash
# Make the script executable
chmod +x start_postgres.sh

# Start PostgreSQL
./start_postgres.sh
```

This will start a PostgreSQL server with:
- Username: postgres
- Password: Password123
- Database: csvdata
- Port: 5432
- Data stored in ./data directory

### Bulk Loading CSV Files Directly (Alternative Approach)

For bulk loading data directly without using temporary tables, you can use the following approach:

#### Basic Usage

```bash
# Load specific files
python csv_to_postgres.py file1.csv file2.csv

# Load all CSV files in a directory
python csv_to_postgres.py "data/*.csv"

# Recursively load all CSV files in subdirectories
python csv_to_postgres.py "data/**/*.csv"

# Load files matching multiple patterns
python csv_to_postgres.py "data/2023-*.csv" "backups/*.csv"

# With database options
python csv_to_postgres.py "data/*.csv" --dbname mydb --user myuser

# Limit the number of rows processed per file
python csv_to_postgres.py "data/*.csv" --limit 1000
```

#### Pattern Matching Rules

- `*` matches any sequence of characters within a directory
- `**` matches any sequence of characters, including directory separators
- `?` matches any single character
- `[seq]` matches any character in seq
- `[!seq]` matches any character not in seq

#### Arguments

- `csv_file` - One or more CSV files or patterns to load (supports wildcards like `*.csv`)
- `--host` - PostgreSQL host (default: `localhost`)
- `--port` - PostgreSQL port (default: `5432`)
- `--dbname` - PostgreSQL database name (default: `csvdata`)
- `--user` - PostgreSQL username (default: `postgres`)
- `--password` - PostgreSQL password (default: `Password123`)
- `--table` - Table name to load data into (default: `csv_data`)
- `--limit` - Optional: Number of rows to process per file (default: all, processes entire files)

#### Processing Details

- Files are processed in alphabetical order
- Progress is shown for each file being processed
- Errors in one file won't stop processing of other files
- Duplicate files (matched by multiple patterns) are processed only once

### Obfuscating CSV Files

```bash
python csv_obfuscator.py input.csv output.csv --columns col1,col2 [--rows N] [--method hash|random|mask]
```

#### Arguments

- `input.csv` - Input CSV file path
- `output.csv` - Output CSV file path
- `--columns` - Comma-separated list of column names to obfuscate
- `--rows` - Optional: Number of rows to process (default: all)
- `--method` - Optional: Obfuscation method (hash, random, mask) (default: hash)
- `--delimiter` - Optional: CSV delimiter (default: ',')
- `--quotechar` - Optional: CSV quote character (default: '"')

#### Obfuscation Methods

1. **hash** - Replaces values with their SHA-256 hash (default)
2. **random** - Replaces values with random characters of the same length
3. **mask** - Keeps first and last characters, replaces middle with asterisks

## Usage Examples

### Basic CSV Loading

Load all rows from multiple CSV files:
```bash
python csv_to_postgres.py data1.csv data2.csv
```

### Querying the Data

Example queries using the indexed fields:

```sql
-- Find by ISIN
SELECT * FROM csv_data WHERE data->>'ID_ISIN' = 'US0378331005';

-- Find by CUSIP
SELECT * FROM csv_data WHERE data->>'ID_CUSIP' = '037833100';

-- Find by SEDOL
SELECT * FROM csv_data WHERE data->>'ID_SEDOL1' = '2046251' OR data->>'ID_SEDOL2' = '2046251';

-- Find by Company ID
SELECT * FROM csv_data WHERE data->>'ID_BB_GLOBAL_COMPANY' = 'BBG001S5S8Y8';

-- Get specific fields
SELECT 
    id_bb_global,
    data->>'ID_ISIN' as isin,
    data->>'NAME' as company_name
FROM csv_data 
WHERE data->>'ID_ISIN' IS NOT NULL;
```

### Adding New Indexes

To add a new index on an additional JSONB field, modify the `index_fields` list in the `create_table_if_not_exists` function in `csv_to_postgres.py`:

```python
index_fields = [
    'ID_BB_GLOBAL_COMPANY',
    'ID_ISIN',
    'ID_CUSIP',
    'ID_SEDOL1',
    'ID_SEDOL2',
    'NEW_FIELD_NAME'  # Add your new field here
]
```

Then, either:
1. Drop and recreate the table (if this is a development environment)
2. Or manually create the index:

```sql
CREATE INDEX idx_csv_data_new_field ON csv_data USING GIN ((data -> 'NEW_FIELD_NAME'));
```

### Handling Large Datasets

For very large datasets, consider these optimizations:

1. **Batch Processing**: The script already processes data in batches of 1000 rows
2. **Temporary Disable Indexes**: For initial loads, you might want to:
   - Drop indexes before loading
   - Load the data
   - Recreate indexes

```sql
-- Before loading
DROP INDEX idx_csv_data_id_isin;
-- Load your data here
-- After loading
CREATE INDEX idx_csv_data_id_isin ON csv_data USING GIN ((data -> 'ID_ISIN'));
```

## Performance Considerations

1. **File Processing**:
   - Files are processed in batches of 1000 rows for efficiency
   - Progress is shown for large files
   - Memory usage is optimized by processing files in chunks

2. **Database Operations**:
   - Each index adds overhead on inserts/updates
   - Batch processing minimizes database round-trips
   - Transactions are used to ensure data consistency

3. **Query Performance**:
   - PostgreSQL will use the most appropriate index for each query
   - Regular VACUUM ANALYZE is recommended for optimal performance
   - Consider using a connection pooler like PgBouncer for high-throughput applications

4. **Large Dataset Tips**:
   - For initial loads of large datasets, consider dropping indexes first and recreating them after loading
   - Use the `--limit` parameter to test with a subset of data
   - Process files in smaller batches if memory usage is a concern

Load only the first 1000 rows from each file:
```bash
python csv_to_postgres.py data1.csv data2.csv --limit 1000
```

### Obfuscate CSV Files

Obfuscate email and phone columns:
```bash
python csv_obfuscator.py data.csv obfuscated_data.csv --columns email,phone
```

Process only the first 1000 rows:
```bash
python csv_obfuscator.py data.csv obfuscated_data.csv --columns email,phone --rows 1000
```

## Performance

Both scripts are designed to handle large files efficiently:
- Process files line by line to minimize memory usage
- Report progress periodically
- Can be stopped after processing a specific number of rows

# CSV Column Analyzer

A high-performance Python script for analyzing unique values in CSV columns. This tool efficiently processes large CSV files and generates a summary of value frequencies for any specified column.

## Features

- Fast CSV processing using optimized pandas and pyarrow
- Colorized console output for better readability
- Detailed processing metrics and statistics
- Automatic timestamped log file generation
- Support for large files with efficient memory usage
- Optional key tracking across files
- Descriptive temporary table naming

## Usage

```bash
python analyze_column.py [options] <csv_file> <column_name>
```

### Arguments

- `csv_file`: Path to the CSV file to analyze
- `column_name`: Name of the column to analyze

### Options

- `--min-count MIN_COUNT`: Only show values that appear at least MIN_COUNT times (default: 1)
- `--track-keys`: Enable tracking of primary keys across files (optional)

## Examples

```bash
# Basic usage
python analyze_column.py data.csv "column_name"

# Only show values that appear at least 5 times
python analyze_column.py data.csv "column_name" --min-count 5

# Enable key tracking
python analyze_column.py data.csv "column_name" --track-keys
```

## Output

The script provides:
1. A count of unique values in the specified column
2. Total number of rows processed
3. Processing time
4. A timestamped log file with detailed metrics
5. If key tracking is enabled, a CSV file with key tracking results

## Temporary Table Naming

When processing files, the script creates temporary tables with descriptive names following this format:
```
temp_{sanitized_filename}_{hash}
```

Where:
- `sanitized_filename`: The original filename with:
  - File extension removed
  - Non-alphanumeric characters converted to underscores
  - Multiple consecutive underscores replaced with a single underscore
  - Leading/trailing underscores removed
- `hash`: A short 8-character MD5 hash of the original filename

Examples:
- `my-data-2023.csv` → `temp_my_data_2023_a1b2c3d4`
- `company--info--2023.csv` → `temp_company_info_2023_e5f6g7h8`
- `data@2023#special.csv` → `temp_data_2023_special_i9j0k1l2`

## Requirements

- Python 3.7+
- pandas >= 2.1.0
- pyarrow >= 14.0.1
- colorama >= 0.4.6

## Installation

1. Clone this repository or download the script
2. Install the required dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Analyzing CSV Files

```bash
python analyze_column.py input.csv output.csv "column_name" [--min-count N]
```

#### Arguments

- `input.csv`: Path to your input CSV file
- `output.csv`: Path where the results will be saved
- `column_name`: Name of the column to analyze (must match exactly with the column header in your CSV)
- `--min-count`: Optional: Minimum count threshold for values to be included in output (default: 1)

#### Examples

Basic usage:
```bash
python analyze_column.py data.csv results.csv "city"
```

Filter out values that appear only once:
```bash
python analyze_column.py data.csv results.csv "city" --min-count 2
```

### Merging Analysis Results

To combine multiple analysis output files and aggregate their counts:

```bash
python merge_analysis.py input_file1.csv input_file2.csv ... output.csv
```

Or using glob patterns:

```bash
python merge_analysis.py "analysis_*.csv" output.csv
```

#### Arguments

- `input_files`: One or more input files to merge. Can be:
  - Explicit file paths (e.g., `file1.csv file2.csv`)
  - Glob patterns (e.g., `"analysis_*.csv"`)
  - Mix of both
- `output_file`: Path to save the merged output

#### Examples

Merge specific files:
```bash
python merge_analysis.py analysis_2023_01.csv analysis_2023_02.csv merged_2023.csv
```

Merge using glob pattern:
```bash
python merge_analysis.py "analysis_*.csv" merged_results.csv
```

Mix of explicit files and patterns:
```bash
python merge_analysis.py analysis_2023_01.csv "analysis_2023_*.csv" merged_2023.csv
```

### Output

#### Analysis Output

The analysis script generates two types of output:

1. **Console Output**: A colorized summary showing:
   - Total number of rows processed
   - Number of unique values found
   - Number of values meeting the minimum count threshold
   - Number of excluded values (when min-count > 1)

   Example console output:
   ```
   Analyzing column 'city' in data.csv...

   === Analysis Summary ===
   Total Rows Processed: 1,234,567
   Unique Values Found: 15,432
   Values with Count ≥ 2: 12,345
   Values Excluded (Count < 2): 3,087

   Results saved to: results.csv
   ```

2. **CSV File**: A CSV file with two columns:
   - `value`: The unique values found in the specified column
   - `count`: The number of occurrences for each value

   Example CSV output:
   ```csv
   value,count
   New York,150
   London,120
   Paris,80
   ```

#### Merge Output

The merge script generates:

1. **Console Output**: A colorized summary showing:
   - Total number of files processed
   - Total number of unique values after merging
   - Total count across all files

   Example console output:
   ```
   Processing 3 analysis files...
   Reading file 1/3: analysis_2023_01.csv
   Reading file 2/3: analysis_2023_02.csv
   Reading file 3/3: analysis_2023_03.csv

   === Merge Summary ===
   Total Files Processed: 3
   Total Unique Values: 1,234
   Total Count: 56,789

   Results saved to: merged_results.csv
   ```

2. **CSV File**: A merged CSV file with aggregated counts:
   - `value`: The unique values found across all files
   - `count`: The sum of counts for each value

   Example merged CSV output:
   ```csv
   value,count
   New York,450
   London,360
   Paris,240
   ```

### Color Scheme

The console output uses colors to make information easily scannable:
- Cyan: Headers and progress messages
- Green: Success messages and total rows/files
- Yellow: Unique values count and file processing
- Magenta: Total counts
- Red: Errors and excluded values

## Performance Optimizations

The scripts are optimized for processing large CSV files through:
- Selective column reading (only loads the required column)
- PyArrow backend for efficient memory usage
- Streaming data processing
- Optimized data structures for counting
- Efficient merging of multiple analysis results

## Error Handling

The scripts include error handling for:
- Missing input files
- Invalid column names
- CSV parsing errors
- File permission issues
- Invalid file formats
- Pattern matching errors

All errors are displayed in red for easy identification.

## Contributing

Feel free to submit issues and enhancement requests!
