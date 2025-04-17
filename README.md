# DuckDB S3 Query System

A distributed query system that allows you to load and query data from S3 using DuckDB. The system consists of two types of containers:
1. Data Containers - Load and serve data from S3
2. Distributed Query Server - Join and query data across multiple data containers

## Features

- Load gzipped CSV files from S3 into memory for fast querying
- Custom table names for each data container
- Distributed querying across multiple data containers
- Automatic query parsing and execution
- Performance metrics and timing information
- REST API interface with OpenAPI documentation

## Prerequisites

- Docker
- AWS credentials (for S3 access)
- Python 3.9+ (for local development)

## Building the Containers

### Data Container

```bash
# Build the data container image
docker build -t duckdb-s3-query -f Dockerfile .
```

### Distributed Query Server

```bash
# Build the distributed query server image
docker build -t duckdb-distributed-query -f Dockerfile.distributed .
```

## Configuration

Create a `config.json` file to map table names to data container URLs and ports:

```json
{
    "tables": {
        "labels": {
            "url": "http://localhost",
            "port": 8001,
            "table_name": "labels"
        },
        "label_sublabels": {
            "url": "http://localhost",
            "port": 8002,
            "table_name": "label_sublabels"
        },
        "label_urls": {
            "url": "http://localhost",
            "port": 8003,
            "table_name": "label_urls"
        }
    }
}
```

## Running the System

### Port Configuration

The system uses Uvicorn as the underlying web server. When running in Docker, you need to map the container's internal port to your desired external port. The internal port is always 8000, but you can map it to any external port you want.

For example, to run a data container on port 8001:

```bash
docker run -d \
  -p 8001:8000 \  # Maps external port 8001 to internal port 8000
  -e AWS_ACCESS_KEY_ID=your_access_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret_key \
  duckdb-s3-query \
  s3://your-bucket/data.csv.gz \
  --table-name my_table
```

### 1. Start Data Containers

Each data container needs to run on a different port. Here's how to start them:

```bash
# Start the first data container (labels)
docker run -d \
  -p 8001:8000 \  # Maps external port 8001 to internal port 8000
  -e AWS_ACCESS_KEY_ID=your_access_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret_key \
  duckdb-s3-query \
  s3://your-bucket/labels.csv.gz \
  --table-name labels

# Start the second data container (label_sublabels)
docker run -d \
  -p 8002:8000 \  # Maps external port 8002 to internal port 8000
  -e AWS_ACCESS_KEY_ID=your_access_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret_key \
  duckdb-s3-query \
  s3://your-bucket/label_sublabels.csv.gz \
  --table-name label_sublabels

# Start the third data container (label_urls)
docker run -d \
  -p 8003:8000 \  # Maps external port 8003 to internal port 8000
  -e AWS_ACCESS_KEY_ID=your_access_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret_key \
  duckdb-s3-query \
  s3://your-bucket/label_urls.csv.gz \
  --table-name label_urls
```

### 2. Start the Distributed Query Server

```bash
# Start the distributed query server
docker run -d \
  -p 8000:8000 \  # Maps external port 8000 to internal port 8000
  -v $(pwd)/config.json:/app/config.json \
  duckdb-distributed-query
```

## Usage Examples

### 1. Query a Single Table

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT * FROM labels LIMIT 5"
  }'
```

### 2. Join Multiple Tables

```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT l.label_id, l.name, ls.sublabel_id, lu.url FROM labels l JOIN label_sublabels ls ON l.label_id = ls.label_id JOIN label_urls lu ON ls.sublabel_id = lu.sublabel_id WHERE l.name LIKE '\''%test%'\''"
  }'
```

### 3. Get Metadata

```bash
# Get metadata from a data container
curl http://localhost:8001/metadata

# Get available tables from distributed server
curl http://localhost:8000/
```

## API Documentation

Once the servers are running, you can access the API documentation at:
- Data Containers: `http://localhost:8001/docs` (or respective port)
- Distributed Server: `http://localhost:8000/docs`

## Response Format

### Query Response

```json
{
  "results": [
    {
      "label_id": 1,
      "name": "test_label",
      "sublabel_id": 100,
      "url": "http://example.com"
    }
  ],
  "columns": ["label_id", "name", "sublabel_id", "url"],
  "execution_time_ms": 123.45,
  "timestamp": "2024-03-14T12:34:56.789Z",
  "source_tables": ["labels", "label_sublabels", "label_urls"]
}
```

### Metadata Response

```json
{
  "s3_url": "s3://your-bucket/data.csv.gz",
  "view_name": "labels",
  "columns": [
    {
      "name": "label_id",
      "type": "BIGINT"
    },
    {
      "name": "name",
      "type": "VARCHAR"
    }
  ],
  "row_count": 1000000,
  "column_count": 2,
  "size_bytes": 15000000,
  "load_time_ms": 1234.56,
  "timestamp": "2024-03-14T12:34:56.789Z"
}
```

## Performance Considerations

1. Data containers load the entire dataset into memory for fast querying
2. The distributed query server:
   - Fetches only required columns from each data container
   - Uses temporary tables for efficient joining
   - Cleans up temporary tables after query execution
3. All operations include timing metrics for monitoring performance

## Error Handling

The system provides detailed error messages for:
- Invalid table names
- Missing tables in configuration
- Query syntax errors
- S3 access issues
- Connection problems between containers

## Development

For local development:

```bash
# Install dependencies
pip install -r requirements.txt

# Run a data container
python query_s3.py s3://your-bucket/data.csv.gz --table-name my_table

# Run the distributed query server
python distributed_query.py --config config.json
```

## License

MIT License
