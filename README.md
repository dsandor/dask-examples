# DuckDB S3 Query API

A Docker-based service that provides a REST API for querying gzipped CSV files stored in Amazon S3 using DuckDB. This service allows you to run SQL queries against your S3 data without downloading it first.

## Features

- Query gzipped CSV files directly from S3
- REST API interface with FastAPI
- Interactive API documentation (Swagger UI and ReDoc)
- AWS credentials management via environment variables
- Secure container configuration
- Support for all DuckDB SQL features
- JSON request/response format
- Error handling with proper HTTP status codes
- Dataset metadata endpoint

## Prerequisites

- Docker
- AWS S3 bucket with gzipped CSV files
- AWS credentials configured via environment variables or IAM roles

## AWS Credentials

The service uses standard AWS environment variables for authentication. There are several ways to provide these credentials to the container:

### 1. Using System AWS Credentials

If you have AWS credentials configured on your system (e.g., through `aws configure`), you can map them directly to the container:

```bash
# Using AWS credentials file
docker run -p 8000:8000 \
  --env-file ~/.aws/credentials \
  duckdb-s3-query \
  s3://your-bucket/your-file.csv.gz

# Or using environment variables from your shell
docker run -p 8000:8000 \
  -e AWS_ACCESS_KEY_ID \
  -e AWS_SECRET_ACCESS_KEY \
  -e AWS_SESSION_TOKEN \
  -e AWS_DEFAULT_REGION \
  duckdb-s3-query \
  s3://your-bucket/your-file.csv.gz
```

### 2. Using AWS CLI Profile

If you have multiple AWS profiles configured:

```bash
# Create a temporary credentials file from your profile
aws configure export-credentials --profile your-profile > /tmp/aws-creds

# Run the container with the temporary credentials
docker run -p 8000:8000 \
  --env-file /tmp/aws-creds \
  duckdb-s3-query \
  s3://your-bucket/your-file.csv.gz

# Clean up
rm /tmp/aws-creds
```

### 3. Using AWS IAM Roles

When running on AWS infrastructure (e.g., EC2, ECS, EKS), you can use IAM roles:

```bash
# No need to pass credentials - the container will use the instance's IAM role
docker run -p 8000:8000 \
  duckdb-s3-query \
  s3://your-bucket/your-file.csv.gz
```

## Building the Docker Image

```bash
docker build -t duckdb-s3-query .
```

## Running the Container

Basic usage with environment variables:
```bash
docker run -p 8000:8000 \
  -e AWS_ACCESS_KEY_ID=your_access_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret_key \
  -e AWS_DEFAULT_REGION=your_region \
  duckdb-s3-query \
  s3://your-bucket/your-file.csv.gz
```

Example files:

```
s3://test-data-dsandor/discogs_20250401_labels.csv.gz
```

```
s3://test-data-dsandor/discogs_20250401_artists.csv.gz
```

### Command Line Arguments

- `s3_url`: (Required) S3 URL of the gzipped CSV file (format: s3://bucket/key)
- `--region`: AWS Region (default: us-east-1)
- `--host`: Host to bind the server to (default: 0.0.0.0)
- `--port`: Port to bind the server to (default: 8000)

## API Usage

### Interactive Documentation

The service provides two interactive documentation interfaces:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### API Endpoints

#### 1. Health Check
```bash
curl http://localhost:8000/
```
Response:
```json
{
    "message": "DuckDB S3 Query API is running",
    "docs_url": "/docs",
    "redoc_url": "/redoc"
}
```

#### 2. Dataset Metadata
```bash
curl http://localhost:8000/metadata
```
Response:
```json
{
    "s3_url": "s3://your-bucket/your-file.csv.gz",
    "view_name": "s3_data",
    "columns": [
        {
            "name": "column1",
            "type": "VARCHAR"
        },
        {
            "name": "column2",
            "type": "INTEGER"
        }
    ],
    "row_count": 1000,
    "column_count": 2,
    "size_bytes": 50000
}
```

#### 3. Execute Query
```bash
curl -X POST "http://localhost:8000/query" \
     -H "Content-Type: application/json" \
     -d '{"query": "SELECT * FROM s3_data LIMIT 5"}'
```

Response format:
```json
{
    "results": [
        {"column1": "value1", "column2": "value2", ...},
        ...
    ],
    "columns": ["column1", "column2", ...]
}
```

### Query Examples

1. Basic SELECT with LIMIT:
```sql
SELECT * FROM s3_data LIMIT 5
```

2. Filtering data:
```sql
SELECT * FROM s3_data WHERE column1 = 'value' LIMIT 10
```

3. Aggregation:
```sql
SELECT column1, COUNT(*) as count 
FROM s3_data 
GROUP BY column1 
ORDER BY count DESC 
LIMIT 5
```

4. Complex queries:
```sql
WITH filtered_data AS (
    SELECT * FROM s3_data 
    WHERE column1 > 100
)
SELECT 
    column2,
    AVG(column3) as avg_value,
    COUNT(*) as record_count
FROM filtered_data
GROUP BY column2
HAVING COUNT(*) > 10
ORDER BY avg_value DESC
LIMIT 10
```

## Security Considerations

1. The container runs as a non-root user for enhanced security
2. AWS credentials are handled securely through environment variables
3. The API doesn't store any data permanently - all queries are executed against the in-memory DuckDB instance
4. The container exposes only port 8000 by default
5. AWS credentials can be managed through Docker secrets or environment variables
6. Consider using IAM roles when running on AWS infrastructure
7. When using `--env-file`, ensure the credentials file has appropriate permissions (600)

## Error Handling

The API returns appropriate HTTP status codes and error messages:

- 400: Bad Request (invalid query syntax, etc.)
- 500: Internal Server Error (database connection issues, etc.)

Example error response:
```json
{
    "detail": "Error message describing what went wrong"
}
```

## Development

The project consists of two main files:

1. `Dockerfile`: Defines the container environment
   - Uses Python 3.9 slim as base image
   - Installs required system and Python packages
   - Sets up security configurations
   - Exposes port 8000

2. `query_s3.py`: The main application
   - FastAPI web server
   - DuckDB integration
   - S3 file handling
   - Query execution
   - Error handling

## Contributing

Feel free to submit issues and enhancement requests!
