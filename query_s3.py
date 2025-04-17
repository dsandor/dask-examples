#!/usr/bin/env python3
import argparse
import duckdb
import boto3
import os
import sys
import time
from datetime import datetime
from urllib.parse import urlparse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn
from typing import Optional, List, Dict

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
    size_bytes: int
    load_time_ms: float
    timestamp: str

# Global variables for DuckDB connection and view name
conn = None
view_name = None  # Will be set from command line argument
s3_url = None
load_start_time = None

def parse_s3_url(s3_url):
    """Parse S3 URL into bucket and key."""
    parsed = urlparse(s3_url)
    if parsed.scheme != 's3':
        raise ValueError("URL must be an S3 URL (s3://bucket/key)")
    bucket = parsed.netloc
    key = parsed.path.lstrip('/')
    return bucket, key

def get_dataset_metadata() -> DatasetMetadata:
    """Get metadata about the loaded dataset."""
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    
    try:
        start_time = time.time()
        
        # Get column information
        columns_info = conn.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = '{view_name}'
            ORDER BY ordinal_position
        """).fetchdf()
        
        columns = [
            ColumnMetadata(name=row['column_name'], type=row['data_type'])
            for _, row in columns_info.iterrows()
        ]
        
        # Get row count
        row_count = conn.execute(f"SELECT COUNT(*) as count FROM {view_name}").fetchone()[0]
        
        # Calculate size based on data types
        size_bytes = 0
        for col in columns:
            # Get a sample value to determine the type
            sample_query = f"""
                SELECT typeof("{col.name}") as col_type
                FROM {view_name}
                LIMIT 1
            """
            col_type = conn.execute(sample_query).fetchone()[0]
            
            # Assign size based on type
            if col_type == 'VARCHAR':
                # For VARCHAR, get average length
                length_query = f"""
                    SELECT AVG(length(CAST("{col.name}" AS VARCHAR))) as avg_length
                    FROM {view_name}
                    LIMIT 1000
                """
                avg_length = conn.execute(length_query).fetchone()[0] or 0
                size_bytes += int(avg_length * row_count)
            elif col_type == 'BIGINT':
                size_bytes += 8 * row_count
            elif col_type == 'DOUBLE':
                size_bytes += 8 * row_count
            elif col_type == 'BOOLEAN':
                size_bytes += 1 * row_count
            else:
                # Default size for unknown types
                size_bytes += 8 * row_count
        
        metadata_time = (time.time() - start_time) * 1000  # Convert to milliseconds
        load_time = (time.time() - load_start_time) * 1000 if load_start_time else 0
        
        print(f"\nMetadata Collection Stats:")
        print(f"  Time to collect metadata: {metadata_time:.2f}ms")
        print(f"  Total load time: {load_time:.2f}ms")
        print(f"  Row count: {row_count:,}")
        print(f"  Column count: {len(columns)}")
        print(f"  Estimated size: {size_bytes:,} bytes")
        
        return DatasetMetadata(
            s3_url=s3_url,
            view_name=view_name,
            columns=columns,
            row_count=row_count,
            column_count=len(columns),
            size_bytes=size_bytes,
            load_time_ms=load_time,
            timestamp=datetime.now().isoformat()
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error getting metadata: {str(e)}")

def setup_duckdb(url: str, table_name: str, region: str = 'us-east-1'):
    """Set up DuckDB connection and create view from S3 file."""
    global conn, view_name, s3_url, load_start_time
    
    try:
        load_start_time = time.time()
        print(f"\nInitializing DuckDB connection at {datetime.now().isoformat()}")
        
        # Store the S3 URL for metadata
        s3_url = url
        view_name = table_name
        
        # Parse S3 URL
        bucket, key = parse_s3_url(url)
        
        # Create DuckDB connection
        conn = duckdb.connect(database=':memory:')
        
        # Install and load httpfs extension for S3 access
        print("Installing and loading httpfs extension...")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # Configure S3 credentials from environment variables
        if all(var in os.environ for var in ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']):
            print("Configuring S3 credentials...")
            conn.execute(f"SET s3_region='{region}';")
            conn.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
            conn.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")
            if 'AWS_SESSION_TOKEN' in os.environ:
                conn.execute(f"SET s3_session_token='{os.environ['AWS_SESSION_TOKEN']}';")

        # Create a table and load the data into memory
        print(f"Loading data from S3 file: {url}")
        conn.execute(f"""
            CREATE TABLE {view_name} AS 
            SELECT * FROM read_csv_auto('s3://{bucket}/{key}', compression='gzip')
        """)
        
        setup_time = (time.time() - load_start_time) * 1000  # Convert to milliseconds
        print(f"\nSetup completed in {setup_time:.2f}ms")
        print(f"Data loaded into table '{view_name}'")
        
        return True
    except Exception as e:
        print(f"Error setting up DuckDB: {str(e)}", file=sys.stderr)
        return False

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
        "table_name": view_name
    }

@app.get("/metadata", response_model=DatasetMetadata)
async def get_metadata():
    """Get metadata about the loaded dataset."""
    return get_dataset_metadata()

@app.post("/query", response_model=QueryResponse)
async def execute_query(query_request: QueryRequest):
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection not initialized")
    
    try:
        start_time = time.time()
        print(f"\nExecuting query at {datetime.now().isoformat()}")
        print(f"Query: {query_request.query}")
        
        # Execute the query
        result = conn.execute(query_request.query).fetchdf()
        
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
        raise HTTPException(status_code=400, detail=str(e))

def main():
    parser = argparse.ArgumentParser(description='Start a web server for querying S3 CSV files using DuckDB')
    parser.add_argument('s3_url', help='S3 URL of the gzipped CSV file (s3://bucket/key)')
    parser.add_argument('--table-name', default='s3_data', help='Name of the table to create (default: s3_data)')
    parser.add_argument('--region', default='us-east-1', help='AWS Region')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind the server to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind the server to')
    
    args = parser.parse_args()

    # Set up DuckDB connection
    if not setup_duckdb(args.s3_url, args.table_name, args.region):
        sys.exit(1)

    # Start the server
    print(f"\nStarting server on {args.host}:{args.port}")
    print(f"Data available in table '{args.table_name}'")
    uvicorn.run(app, host=args.host, port=args.port)

if __name__ == "__main__":
    main() 