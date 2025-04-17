#!/usr/bin/env python3
import argparse
import duckdb
import boto3
import os
import sys
import time
import hashlib
import shutil
from datetime import datetime, timezone
from urllib.parse import urlparse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from typing import Optional, List, Dict
import logging
import pathlib

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set S3/boto3 logging to WARNING to reduce verbosity
logging.getLogger('boto3').setLevel(logging.WARNING)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('s3transfer').setLevel(logging.WARNING)

# Enable DuckDB query logging
logging.getLogger('duckdb').setLevel(logging.DEBUG)

class QueryRequest(BaseModel):
    query: str

class QueryResponse(BaseModel):
    results: list
    columns: list
    execution_time_ms: float
    timestamp: str

class ColumnMetadata(BaseModel):
    name: str
    type: str

class DatasetMetadata(BaseModel):
    s3_url: str
    view_name: str
    columns: List[ColumnMetadata]
    row_count: int
    column_count: int
    local_cache_path: Optional[str] = None
    last_modified: Optional[str] = None
    cache_status: Optional[str] = None

# Global variables for DuckDB connection and view name
conn = None
view_name = None  # Will be set from command line argument
s3_url = None
load_time_ms = None
local_cache_path = None
last_modified = None
cache_status = None

def parse_s3_url(s3_url):
    """Parse S3 URL into bucket and key."""
    parsed = urlparse(s3_url)
    if parsed.scheme != 's3':
        raise ValueError("URL must be an S3 URL (s3://bucket/key)")
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key

def get_dataset_metadata() -> DatasetMetadata:
    """Get metadata about the dataset."""
    try:
        # Get column information without loading all data
        column_info = conn.execute(f"DESCRIBE {view_name}").fetchdf()
        
        # Get row count using a count query
        row_count = conn.execute(f"SELECT COUNT(*) as count FROM {view_name}").fetchone()[0]
        
        # Create column metadata
        columns = [
            ColumnMetadata(name=row["column_name"], type=row["column_type"])
            for _, row in column_info.iterrows()
        ]
        
        return DatasetMetadata(
            s3_url=s3_url,
            view_name=view_name,
            columns=columns,
            row_count=row_count,
            column_count=len(columns),
            local_cache_path=local_cache_path,
            last_modified=last_modified,
            cache_status=cache_status
        )
    except Exception as e:
        logger.error(f"Error getting metadata: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

def get_s3_file_metadata(bucket: str, key: str, s3_client) -> Dict:
    """Get metadata about an S3 file using HEAD operation."""
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        return {
            'last_modified': response['LastModified'].isoformat(),
            'etag': response.get('ETag', '').strip('"'),
            'size': response['ContentLength']
        }
    except Exception as e:
        logger.error(f"Error getting S3 file metadata: {str(e)}")
        return None

def download_s3_file(bucket: str, key: str, local_path: str, s3_client) -> bool:
    """Download a file from S3 to a local path."""
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        
        # Download the file
        logger.info(f"Downloading S3 file s3://{bucket}/{key} to {local_path}")
        s3_client.download_file(bucket, key, local_path)
        return True
    except Exception as e:
        logger.error(f"Error downloading S3 file: {str(e)}")
        return False

def setup_duckdb(url: str, table_name: str) -> None:
    """Set up DuckDB connection and load data from S3 or local cache"""
    global conn, view_name, s3_url, load_time_ms, local_cache_path, last_modified, cache_status
    
    s3_url = url
    view_name = table_name
    
    start_time = time.time()
    
    # Parse S3 URL
    if not url.startswith('s3://'):
        raise ValueError("URL must start with s3://")
    
    parts = url[5:].split('/', 1)
    if len(parts) != 2:
        raise ValueError("Invalid S3 URL format. Expected s3://bucket/path")
    
    bucket, key = parts
    
    # Set up AWS credentials from environment variables
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    aws_region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    aws_session_token = os.environ.get('AWS_SESSION_TOKEN')
    
    # Validate AWS credentials
    if not aws_access_key or not aws_secret_key:
        raise ValueError(
            "AWS credentials not found in environment variables. "
            "Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. "
            "If using temporary credentials, also set AWS_SESSION_TOKEN."
        )
    
    # Create S3 client with explicit credentials
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            aws_session_token=aws_session_token,
            region_name=aws_region
        )
        
        # Test S3 access by listing bucket contents
        s3_client.head_bucket(Bucket=bucket)
    except Exception as e:
        error_msg = str(e)
        if '403' in error_msg:
            raise ValueError(
                f"Access denied to S3 bucket '{bucket}'. "
                "Please check your AWS credentials and bucket permissions. "
                f"Error: {error_msg}"
            )
        elif '404' in error_msg:
            raise ValueError(f"S3 bucket '{bucket}' not found. Please check the bucket name.")
        else:
            raise ValueError(f"Error accessing S3: {error_msg}")
    
    # Create cache directory
    cache_dir = os.path.join('/data/cache')
    os.makedirs(cache_dir, exist_ok=True)
    
    # Use the original filename from S3
    filename = key.split('/')[-1]  # Get the last part of the S3 path
    local_cache_path = os.path.join(cache_dir, filename)
    
    # Check if we have the file cached
    if os.path.exists(local_cache_path):
        logger.info(f"Using cached file: {local_cache_path}")
        need_download = False
        cache_status = "HIT"
    else:
        logger.info(f"No cached file found at: {local_cache_path}")
        need_download = True
        cache_status = "MISS"
    
    # Download the file if needed
    if need_download:
        try:
            logger.info(f"Downloading S3 file s3://{bucket}/{key} to {local_cache_path}")
            s3_client.download_file(bucket, key, local_cache_path)
            cache_status = "DOWNLOADED"
        except Exception as e:
            error_msg = str(e)
            if '403' in error_msg:
                raise ValueError(
                    f"Access denied to S3 object '{key}' in bucket '{bucket}'. "
                    "Please check your AWS credentials and object permissions. "
                    f"Error: {error_msg}"
                )
            elif '404' in error_msg:
                raise ValueError(f"S3 object '{key}' not found in bucket '{bucket}'. Please check the object path.")
            else:
                raise ValueError(f"Error downloading from S3: {error_msg}")
    
    # Create DuckDB connection
    conn = duckdb.connect(database=':memory:')
    
    # Enable query logging and progress bar
    conn.execute("SET enable_progress_bar=true")
    
    # Create a view that will lazily load the data from local cache only
    logger.info(f"Creating lazy-loaded view from local cache: {local_cache_path}")
    
    # First, create a temporary table to store the schema
    conn.execute(f"""
        CREATE TEMP TABLE {view_name}_schema AS 
        SELECT * FROM read_csv_auto('{local_cache_path}',
            compression='gzip',
            auto_detect=true,
            header=true,
            sample_size=-1
        ) LIMIT 0
    """)
    
    # Then create the view that reads from the local file
    conn.execute(f"""
        CREATE VIEW {view_name} AS 
        SELECT * FROM read_csv_auto('{local_cache_path}',
            compression='gzip',
            auto_detect=true,
            header=true,
            sample_size=-1
        )
    """)
    
    # Drop the temporary schema table
    conn.execute(f"DROP TABLE {view_name}_schema")
    
    load_time_ms = (time.time() - start_time) * 1000
    logger.info(f"View created in {load_time_ms:.2f}ms (Cache status: {cache_status})")

app = FastAPI(
    title="DuckDB S3 Query API",
    description="API for querying gzipped CSV files stored in S3 using DuckDB",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {
        "message": "DuckDB S3 Query API is running",
        "docs_url": "/docs",
        "redoc_url": "/redoc",
        "table_name": view_name,
        "cache_status": cache_status,
        "local_cache_path": local_cache_path
    }

@app.get("/metadata", response_model=DatasetMetadata)
async def get_metadata():
    """Get metadata about the dataset."""
    return get_dataset_metadata()

@app.post("/query", response_model=QueryResponse)
async def execute_query(query_request: QueryRequest):
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    
    try:
        start_time = time.time()
        print(f"\nExecuting query at {datetime.now().isoformat()}")
        print(f"Query: {query_request.query}")
        
        # Execute the query with lazy loading from local cache
        logger.debug(f"Executing DuckDB query: {query_request.query}")
        result = conn.execute(query_request.query).fetchdf()
        logger.debug(f"Query returned {len(result)} rows")
        
        # Convert results to list of dictionaries
        results = result.to_dict('records')
        columns = list(result.columns)
        
        execution_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        print(f"\nQuery Execution Stats:")
        print(f"  Execution time: {execution_time:.2f}ms")
        print(f"  Rows returned: {len(results):,}")
        print(f"  Columns: {', '.join(columns)}")
        
        return QueryResponse(
            results=results,
            columns=columns,
            execution_time_ms=execution_time,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

def main():
    parser = argparse.ArgumentParser(description='Start a web server for querying S3 CSV files using DuckDB')
    parser.add_argument('s3_url', help='S3 URL of the gzipped CSV file (s3://bucket/key)')
    parser.add_argument('--table-name', default='s3_data', help='Name of the table to create (default: s3_data)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind the server to')
    parser.add_argument('--force-download', action='store_true', help='Force download from S3 even if cached file exists')
    
    args = parser.parse_args()

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Set up DuckDB connection
    setup_duckdb(args.s3_url, args.table_name)

    # Start the server
    print(f"\nStarting server on {args.host}:{args.port}")
    print(f"Data available in table '{args.table_name}'")
    print(f"Cache status: {cache_status}")
    print(f"Local cache path: {local_cache_path}")
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main() 