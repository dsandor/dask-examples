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

### Loading CSV Files into PostgreSQL

The loader supports wildcard patterns for loading multiple files at once, making it easy to process entire directories of files.

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
