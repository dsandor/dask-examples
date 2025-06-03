# Missing IDs Checker

A Python script to identify missing IDs between temporary tables and a reference table in PostgreSQL databases.

## Overview

This script checks all tables in a PostgreSQL database that start with `temp_` and compares their `ID_BB_GLOBAL` column values against the `id_bb_global` column in the `csv_data` table. It identifies any IDs that exist in the temporary tables but are missing from the reference table.

## Features

- Automatically discovers all tables with `temp_` prefix
- Case-sensitive column name handling
- Efficient SQL queries using LEFT JOIN
- Comprehensive logging to both file and console
- Detailed error handling and reporting
- Memory-efficient processing of large datasets

## Requirements

- Python 3.6+
- PostgreSQL database
- Required Python packages:
  - psycopg2
  - pandas
  - typing

## Installation

1. Ensure you have Python 3.6 or higher installed
2. Install required packages:
```bash
pip install psycopg2-binary pandas
```

## Configuration

Update the database connection parameters in the script:

```python
db_params = {
    'dbname': 'your_database',
    'user': 'your_user',
    'password': 'your_password',
    'host': 'your_host',
    'port': '5432'
}
```

## Usage

1. Update the database connection parameters in the script
2. Run the script:
```bash
python check_missing_ids.py
```

## Output

The script generates two types of output:

1. Console output showing real-time progress
2. A timestamped log file (format: `missing_ids_check_YYYYMMDD_HHMMSS.log`)

The log file contains:
- Connection status
- Number of temp tables found
- Progress for each table check
- Summary of missing IDs found
- Any errors or exceptions that occurred

## Example Output

```
2024-03-14 10:15:30 - INFO - Successfully connected to database
2024-03-14 10:15:30 - INFO - Found 3 temp tables
2024-03-14 10:15:30 - INFO - Checking table: temp_table1
2024-03-14 10:15:31 - INFO - Found 5 missing IDs in temp_table1
2024-03-14 10:15:31 - INFO - Checking table: temp_table2
2024-03-14 10:15:32 - INFO - No missing IDs found in temp_table2
```

## Error Handling

The script includes comprehensive error handling for:
- Database connection failures
- Query execution errors
- Table access issues
- Data type mismatches

All errors are logged with detailed information to help with troubleshooting.

## Performance Considerations

- Uses efficient SQL queries with LEFT JOIN
- Processes one table at a time to manage memory usage
- Uses DISTINCT to avoid duplicate processing
- Implements proper connection cleanup

## Contributing

Feel free to submit issues and enhancement requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details. 