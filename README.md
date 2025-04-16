# Large CSV Query API with Dask

A high-performance API for querying large CSV datasets using Dask and FastAPI. This solution is optimized for handling multi-gigabyte CSV files with minimal memory usage and maximum performance.

## Features

- **Distributed Processing**: Utilizes all available CPU cores for parallel data processing
- **Memory Efficient**: Processes data in chunks without loading the entire dataset into memory
- **Fast Queries**: Optimized pagination and filtering directly on Dask DataFrames
- **Interactive Documentation**: Swagger UI for easy API exploration
- **Detailed Logging**: Comprehensive logging for performance monitoring

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd bigdata

# Install dependencies
pip install -r requirements.txt
```

## Usage

```bash
# Start the API server
python load_large_csv_with_dask.py
```

The server will start on `http://localhost:8000` by default.

## API Endpoints

### 1. Get File Information

```
GET /
```

Returns metadata about the loaded CSV file, including file size, number of partitions, columns, and loading time.

**Example:**
```bash
curl http://localhost:8000/
```

### 2. List Columns

```
GET /columns
```

Returns a list of all column names in the CSV file.

**Example:**
```bash
curl http://localhost:8000/columns
```

### 3. Get Data Sample

```
GET /sample?n={number_of_rows}
```

Returns a sample of rows from the CSV file.

**Parameters:**
- `n` (optional): Number of rows to sample (default: 10)

**Example:**
```bash
curl http://localhost:8000/sample?n=5
```

### 4. Query Data

```
GET /query?columns={columns}&filters={filters}&limit={limit}&offset={offset}
```

Query the CSV data with column selection, filters, and pagination.

**Parameters:**
- `columns` (optional): Comma-separated list of columns to include
- `filters` (optional): Filters in format column:value (comma-separated for multiple)
- `limit` (optional): Maximum number of rows to return (default: 100)
- `offset` (optional): Number of rows to skip (default: 0)

**Example Queries:**

Basic query with limit:
```bash
curl http://localhost:8000/query?limit=10
```

Select specific columns:
```bash
curl http://localhost:8000/query?columns=column1,column2,column3&limit=10
```

Filter by column value:
```bash
curl http://localhost:8000/query?filters=column1:value1&limit=10
```

Multiple filters:
```bash
curl http://localhost:8000/query?filters=column1:value1,column2:value2&limit=10
```

Pagination:
```bash
curl http://localhost:8000/query?offset=1000&limit=10
```

Combined query:
```bash
curl http://localhost:8000/query?columns=column1,column2&filters=column1:value1&offset=1000&limit=10
```

### 5. Get Column Statistics

```
GET /stats/{column}
```

Returns statistics for a specific column.

**Parameters:**
- `column`: Name of the column to get statistics for

**Example:**
```bash
curl http://localhost:8000/stats/column1
```

### 6. Get Rows by Index

```
GET /rows/{start_idx}?n={number_of_rows}
```

Retrieves specific rows by index position.

**Parameters:**
- `start_idx`: Starting index position
- `n` (optional): Number of rows to retrieve (default: 10)

**Example:**
```bash
curl http://localhost:8000/rows/1000?n=5
```

### 7. Get Column Information

```
GET /column-info
```

Returns detailed information about the columns in the CSV file, including original names and any renamed columns due to duplicates.

**Example:**
```bash
curl http://localhost:8000/column-info
```

### 8. Search by Account Number

```
GET /account-search?account={account_number}&limit={limit}&offset={offset}
```

Search for transactions where either the "From Account" or "To Account" matches the provided account number.

**Parameters:**
- `account` (required): Account number to search for
- `limit` (optional): Maximum number of rows to return (default: 100)
- `offset` (optional): Number of rows to skip (default: 0)

**Example:**
```bash
curl http://localhost:8000/account-search?account=12345678&limit=10
```

## Handling Duplicate Column Names

The CSV file contains duplicate column names (two "Account" columns). The API automatically handles this by:

1. Detecting duplicate column names during loading
2. Renaming the duplicates with a suffix (e.g., "Account" becomes "Account_3")
3. Providing a mapping between original and renamed columns via the `/column-info` endpoint

When using the `/account-search` endpoint, the API intelligently searches both "From Account" and "To Account" columns, so you don't need to worry about the renaming.

## Example Queries for Your Dataset

Based on your CSV headers:
```
Timestamp,From Bank,Account,To Bank,Account,Amount Received,Receiving Currency,Amount Paid,Payment Currency,Payment Format,Is Laundering
```

Here are some useful queries:

### 1. Search for transactions by account number (in either From or To account)

```bash
curl http://localhost:8000/account-search?account=12345678
```

### 2. View all columns with their renamed versions

```bash
curl http://localhost:8000/column-info
```

### 3. Filter transactions by bank

```bash
curl http://localhost:8000/query?filters=From%20Bank:HSBC
```

### 4. Find potential money laundering transactions

```bash
curl http://localhost:8000/query?filters=Is%20Laundering:True
```

### 5. Get transactions with specific payment format

```bash
curl http://localhost:8000/query?filters=Payment%20Format:SWIFT
```

### 6. Combine multiple filters

```bash
curl http://localhost:8000/query?filters=From%20Bank:HSBC,Payment%20Currency:USD
```

### 7. Select specific columns only

```bash
curl http://localhost:8000/query?columns=Timestamp,From%20Bank,To%20Bank,Amount%20Paid,Payment%20Currency
```

## Performance Testing Examples

### 1. Test Basic Query Performance

```bash
time curl http://localhost:8000/query?limit=100
```

### 2. Test Pagination Performance

```bash
# Test small offset
time curl http://localhost:8000/query?offset=100&limit=10

# Test medium offset
time curl http://localhost:8000/query?offset=10000&limit=10

# Test large offset
time curl http://localhost:8000/query?offset=1000000&limit=10
```

### 3. Test Filter Performance

```bash
# Single filter
time curl http://localhost:8000/query?filters=column1:value1&limit=10

# Multiple filters
time curl http://localhost:8000/query?filters=column1:value1,column2:value2&limit=10
```

### 4. Test Column Selection Performance

```bash
# Select few columns
time curl http://localhost:8000/query?columns=column1,column2&limit=10

# Select many columns
time curl http://localhost:8000/query?columns=column1,column2,column3,column4,column5&limit=10
```

### 5. Test Combined Query Performance

```bash
time curl http://localhost:8000/query?columns=column1,column2&filters=column1:value1&offset=10000&limit=10
```

## Performance Optimization Tips

1. **Adjust Partition Size**: If queries are still slow, try adjusting the partition size in the code
2. **Increase Worker Memory**: For very large datasets, increase the `memory_limit` parameter
3. **Monitor with Dashboard**: Use the Dask dashboard (URL in logs) to identify bottlenecks
4. **Use Specific Columns**: Always specify only the columns you need for better performance
5. **Filter Early**: Apply filters to reduce the dataset size before pagination

## Troubleshooting

### Query is taking too long

- Try reducing the `limit` parameter
- Use more specific filters to reduce the dataset size
- Check the logs for performance breakdown of each query stage

### Out of Memory Errors

- Reduce the number of partitions being processed simultaneously
- Increase the `memory_limit` parameter for workers
- Use more specific column selection

## Advanced Configuration

The code includes several parameters that can be tuned for specific use cases:

- `n_workers`: Number of parallel workers (defaults to CPU count - 1)
- `threads_per_worker`: Threads per worker (defaults to 2)
- `memory_limit`: Memory limit per worker (defaults to 4GB)
- `partition_size_mb`: Size of each partition (dynamically calculated)

## License

[MIT License](LICENSE)

# S3 Directory Enumerator

This Python script enumerates all subdirectories in an S3 bucket, finding the most recent CSV.GZ files in each directory. It supports filtering directories using regular expressions and outputs detailed information about the files found.

## Features

- Recursively enumerates all directories in a specified S3 bucket
- Finds the most recent CSV.GZ file in each directory
- Supports include/exclude patterns using regular expressions
- Allows specifying a root path within the bucket
- Optional file downloading with preserved directory structure
- Calculates total size of all latest files
- Outputs detailed logs with timestamps
- Generates a JSON report with file information

## Prerequisites

- Python 3.6+
- AWS credentials configured (either through AWS CLI or environment variables)
- Required Python packages:
  - boto3

## Installation

1. Install the required dependencies:
```bash
pip install boto3
```

2. Configure AWS credentials:
   - Using AWS CLI: `aws configure`
   - Or set environment variables:
     ```bash
     export AWS_ACCESS_KEY_ID=your_access_key
     export AWS_SECRET_ACCESS_KEY=your_secret_key
     export AWS_DEFAULT_REGION=your_region
     ```

## Usage

Basic usage:
```bash
python s3_enum.py your-bucket-name
```

With root path:
```bash
python s3_enum.py my-bucket --root-path "path/to/start"
```

With include/exclude patterns:
```bash
python s3_enum.py my-bucket --include "2023.*" --exclude "temp"
```

With file downloading:
```bash
python s3_enum.py my-bucket --download --download-dir ./downloaded_files
```

Combined usage:
```bash
python s3_enum.py my-bucket --root-path "BBUpload/Foo" --download --download-dir ./data
```

With custom output file:
```bash
python s3_enum.py my-bucket --output results.json
```

### Command Line Arguments

- `bucket`: (Required) Name of the S3 bucket to enumerate
- `--root-path`: (Optional) Root path within the bucket to start enumeration
- `--include`: (Optional) Regex pattern to include directories
- `--exclude`: (Optional) Regex pattern to exclude directories
- `--output`: (Optional) Output JSON file path (default: s3_enum_results.json)
- `--download`: (Optional) Flag to enable file downloading
- `--download-dir`: (Required if --download is used) Directory to download files to

## Output

The script generates two types of output:

1. Console logs showing:
   - Directory processing progress
   - Found files and their sizes
   - Download progress (if enabled)
   - Total size of all latest files

2. JSON file containing:
   ```json
   {
     "bucket": "bucket-name",
     "root_path": "path/to/start",
     "total_size": 1234567,
     "latest_files": [
       {
         "path": "s3://bucket-name/path/to/file.csv.gz",
         "size": 123456,
         "last_modified": "2024-03-21T12:34:56.789Z",
         "s3_key": "path/to/file.csv.gz",
         "local_path": "./downloaded_files/path/to/file.csv.gz"
       },
       ...
     ]
   }
   ```

## Directory Structure Preservation

When downloading files:
- The script preserves the directory structure from S3
- The root path is excluded from the local directory structure
- Example:
  - S3 path: `s3://bucket/BBUpload/Foo/bar/file.csv.gz`
  - Root path: `BBUpload/Foo`
  - Local path: `./downloaded_files/bar/file.csv.gz`

## Error Handling

The script includes comprehensive error handling:
- Logs errors for individual file/directory processing failures
- Logs download failures without stopping the process
- Continues processing even if individual operations fail
- Provides detailed error messages in the logs

## Notes

- The script uses pagination to handle large buckets efficiently
- File dates are determined by the LastModified timestamp in S3
- The script processes up to 1000 objects per directory (configurable via MaxKeys)
- All paths in the output are in the format `s3://bucket-name/path/to/file.csv.gz`
- The root path parameter allows you to limit enumeration to a specific directory within the bucket
- Downloaded files maintain the same directory structure as in S3, excluding the root path
