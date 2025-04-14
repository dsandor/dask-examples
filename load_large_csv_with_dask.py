#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
API server for querying HI-Large_Trans.csv using Dask and FastAPI
"""

import dask.dataframe as dd
import pandas as pd
import os
import time
import logging
import uuid
import json
import multiprocessing
from datetime import datetime
from typing import List, Optional, Dict, Any, Union
from fastapi import FastAPI, Query, HTTPException, Path, Depends, Request
from pydantic import BaseModel, Field
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.staticfiles import StaticFiles
from fastapi.openapi.utils import get_openapi
from dask.distributed import Client, LocalCluster

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("api_server.log")
    ]
)
logger = logging.getLogger("csv_query_api")

# Initialize FastAPI app with detailed metadata for Swagger UI
app = FastAPI(
    title="Large CSV Query API",
    description="""
    # Large CSV Query API
    
    This API provides endpoints to query and analyze large CSV files using Dask.
    
    ## Features
    
    * Get information about the loaded CSV file
    * List all columns in the dataset
    * Query data with filters and pagination
    * Get statistics for specific columns
    * Sample data from the dataset
    
    ## How to use
    
    1. Start by checking the file information at the root endpoint `/`
    2. View available columns with the `/columns` endpoint
    3. Get a sample of data with the `/sample` endpoint
    4. Query data with filters using the `/query` endpoint
    """,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add middleware for request logging
@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    start_time = time.time()
    
    # Log request details
    logger.info(f"Request {request_id} started: {request.method} {request.url.path} - Client: {request.client.host}")
    
    # Process the request
    response = await call_next(request)
    
    # Calculate processing time
    process_time = time.time() - start_time
    
    # Log response details
    logger.info(f"Request {request_id} completed: Status {response.status_code} - Took {process_time:.4f} seconds")
    
    # Add custom headers
    response.headers["X-Process-Time"] = str(process_time)
    response.headers["X-Request-ID"] = request_id
    
    return response

# Set up Dask distributed client for parallel processing
# Calculate optimal number of workers based on CPU cores
def setup_dask_client():
    n_cores = multiprocessing.cpu_count()
    n_workers = max(1, n_cores - 1)  # Leave one core for the main process
    
    # Set up a local cluster with optimized settings
    cluster = LocalCluster(
        n_workers=n_workers,
        threads_per_worker=2,  # Use 2 threads per worker for better I/O performance
        memory_limit='4GB',    # Limit memory per worker to avoid OOM errors
        processes=True,        # Use processes instead of threads for better parallelism
        scheduler_port=0,      # Use a random port for the scheduler
        dashboard_address=':0' # Use a random port for the dashboard
    )
    
    client = Client(cluster)
    return client, cluster

# Global variables
ddf = None
client = None
cluster = None
file_info = {
    "file_path": "HI-Large_Trans.csv",
    "file_size_gb": 0,
    "num_partitions": 0,
    "columns": [],
    "loaded": False,
    "load_time": 0,
    "total_rows": 0,
    "row_count_computed": False,
    "n_workers": 0,
    "partition_size_mb": 256,  # Default partition size
    "dashboard_link": "",
    "original_columns": [],  # Store original column names
    "renamed_columns": {}    # Mapping of original to renamed columns
}

# Response models with detailed field descriptions
class FileInfo(BaseModel):
    file_path: str = Field(..., description="Path to the loaded CSV file")
    file_size_gb: float = Field(..., description="Size of the file in gigabytes")
    num_partitions: int = Field(..., description="Number of Dask partitions used for processing")
    columns: List[str] = Field(..., description="List of column names in the CSV file")
    loaded: bool = Field(..., description="Whether the file has been successfully loaded")
    load_time: float = Field(..., description="Time taken to load the file in seconds")
    n_workers: int = Field(..., description="Number of Dask workers for parallel processing")
    dashboard_link: str = Field(..., description="Link to the Dask dashboard for monitoring")
    
    class Config:
        schema_extra = {
            "example": {
                "file_path": "HI-Large_Trans.csv",
                "file_size_gb": 16.5,
                "num_partitions": 65,
                "columns": ["column1", "column2", "column3"],
                "loaded": True,
                "load_time": 5.23,
                "n_workers": 4,
                "dashboard_link": "http://localhost:8787/status"
            }
        }

class QueryResult(BaseModel):
    count: int = Field(..., description="Total number of rows matching the query")
    data: List[Dict[str, Any]] = Field(..., description="Query results as list of records")
    query_time: float = Field(..., description="Time taken to execute the query in seconds")
    
    class Config:
        schema_extra = {
            "example": {
                "count": 150,
                "data": [{"id": 1, "name": "Example"}, {"id": 2, "name": "Example 2"}],
                "query_time": 0.45
            }
        }

class ColumnStats(BaseModel):
    stats: Dict[str, Any] = Field(..., description="Statistics for the column")
    query_time: float = Field(..., description="Time taken to compute statistics in seconds")

# Dependency for checking if file is loaded
async def get_loaded_ddf():
    if not file_info["loaded"]:
        logger.error("Attempted to access data before CSV file was loaded")
        raise HTTPException(status_code=503, detail="CSV file not loaded yet")
    return ddf

@app.on_event("startup")
async def startup_event():
    """Load the CSV file when the API server starts"""
    global ddf, file_info, client, cluster
    
    file_path = file_info["file_path"]
    logger.info(f"Starting server and loading CSV file: {file_path}")
    
    # Set up Dask client for parallel processing
    logger.info("Setting up Dask distributed client for parallel processing")
    try:
        client, cluster = setup_dask_client()
        file_info["n_workers"] = len(client.scheduler_info()["workers"])
        file_info["dashboard_link"] = client.dashboard_link
        logger.info(f"Dask client set up with {file_info['n_workers']} workers")
        logger.info(f"Dask dashboard available at: {file_info['dashboard_link']}")
    except Exception as e:
        logger.error(f"Error setting up Dask client: {str(e)}", exc_info=True)
        # Fall back to local scheduler if distributed setup fails
        logger.info("Falling back to local scheduler")
    
    load_start_time = time.time()
    
    try:
        # Get file size
        file_size_bytes = os.path.getsize(file_path)
        file_info["file_size_gb"] = file_size_bytes / (1024**3)
        logger.info(f"File size: {file_info['file_size_gb']:.2f} GB")
        
        # Calculate optimal partition size based on file size
        # Aim for 100-200 partitions for better parallelism
        target_partitions = min(200, max(100, file_info["n_workers"] * 25))
        partition_size_mb = max(128, int((file_info["file_size_gb"] * 1024) / target_partitions))
        file_info["partition_size_mb"] = partition_size_mb
        blocksize = f"{partition_size_mb}MB"
        
        logger.info(f"Using optimized partition size: {blocksize} for target of {target_partitions} partitions")
        
        # First read just the header to detect duplicate column names
        logger.info("Checking for duplicate column names in CSV header")
        with open(file_path, 'r') as f:
            header_line = f.readline().strip()
            original_columns = header_line.split(',')
            file_info["original_columns"] = original_columns
            
            # Check for duplicate column names and create a mapping
            seen_columns = {}
            renamed_columns = {}
            
            for i, col in enumerate(original_columns):
                if col in seen_columns:
                    # Rename duplicate column
                    new_name = f"{col}_{i}"
                    renamed_columns[i] = new_name
                    logger.info(f"Found duplicate column '{col}' at position {i}, renaming to '{new_name}'")
                else:
                    seen_columns[col] = i
            
            file_info["renamed_columns"] = renamed_columns
            
            # If we have duplicate columns, use custom header handling
            if renamed_columns:
                logger.info(f"Detected {len(renamed_columns)} duplicate column names, using custom header handling")
        
        # Load the CSV file into a Dask DataFrame with optimized settings
        logger.info("Initializing Dask DataFrame with optimized settings")
        
        # Handle duplicate column names by using positional header names
        if file_info["renamed_columns"]:
            # Create a list of column names with duplicates renamed
            header_names = original_columns.copy()
            for idx, new_name in renamed_columns.items():
                header_names[idx] = new_name
            
            # Load with custom header names
            ddf = dd.read_csv(
                file_path, 
                blocksize=blocksize,
                assume_missing=True,
                sample=50000,
                dtype_backend='pyarrow',
                storage_options={'anon': True},
                low_memory=False,
                engine='c',
                header=0,
                names=header_names
            )
            logger.info(f"CSV loaded with renamed columns: {renamed_columns}")
        else:
            # Standard loading without column renaming
            ddf = dd.read_csv(
                file_path, 
                blocksize=blocksize,
                assume_missing=True,
                sample=50000,
                dtype_backend='pyarrow',
                storage_options={'anon': True},
                low_memory=False,
                engine='c'
            )
        
        # Force computation of metadata to verify the file can be read
        logger.info("Computing DataFrame metadata...")
        ddf._meta
        
        # Update file info
        file_info["num_partitions"] = ddf.npartitions
        file_info["columns"] = ddf.columns.tolist()
        file_info["loaded"] = True
        
        load_time = time.time() - load_start_time
        file_info["load_time"] = load_time
        
        logger.info(f"CSV file loaded successfully in {load_time:.2f} seconds")
        logger.info(f"Number of partitions: {ddf.npartitions}")
        logger.info(f"Number of columns: {len(file_info['columns'])}")
        
        # Start a background task to compute row count
        import threading
        def compute_row_count():
            try:
                logger.info("Starting background computation of total row count")
                start_time = time.time()
                
                # Use optimized count method
                if client:
                    # With distributed client, we can count partitions in parallel
                    logger.info("Using parallel count method with distributed client")
                    future = client.submit(len, ddf)
                    count = future.result()
                else:
                    # Without client, use standard count
                    count = len(ddf)
                
                file_info["total_rows"] = count
                file_info["row_count_computed"] = True
                duration = time.time() - start_time
                logger.info(f"Total row count computed: {count} rows in {duration:.2f} seconds")
            except Exception as e:
                logger.error(f"Error computing row count: {str(e)}", exc_info=True)
        
        # Run the computation in a background thread
        threading.Thread(target=compute_row_count, daemon=True).start()
        
        # Persist frequently accessed partitions in memory
        logger.info("Persisting first few partitions in memory for faster access...")
        if client:
            try:
                # Persist the first 5 partitions for faster access to common queries
                first_partitions = ddf.partitions[:5].persist()
                logger.info("First partitions persisted in memory")
            except Exception as e:
                logger.warning(f"Could not persist partitions: {str(e)}")
        
    except FileNotFoundError:
        logger.error(f"Error: File '{file_path}' not found")
        file_info["loaded"] = False
    except Exception as e:
        logger.error(f"Error loading CSV file: {str(e)}", exc_info=True)
        file_info["loaded"] = False

@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources when the server shuts down"""
    global client, cluster
    
    logger.info("Server shutting down")
    
    # Close Dask client and cluster
    if client:
        logger.info("Closing Dask client and cluster")
        try:
            client.close()
            if cluster:
                cluster.close()
            logger.info("Dask resources released")
        except Exception as e:
            logger.error(f"Error closing Dask resources: {str(e)}")

@app.get("/", response_model=FileInfo, tags=["Information"], 
         summary="Get file information",
         description="Returns information about the loaded CSV file including size, columns, and load status")
async def get_file_info():
    """Get information about the loaded CSV file"""
    logger.info("Endpoint called: Get file information")
    return file_info

@app.get("/columns", tags=["Information"], 
         summary="Get column names",
         description="Returns a list of all column names available in the CSV file")
async def get_columns(ddf=Depends(get_loaded_ddf)):
    """Get the list of columns in the CSV file"""
    logger.info("Endpoint called: Get columns")
    return {"columns": file_info["columns"]}

@app.get("/sample", response_model=QueryResult, tags=["Data"], 
         summary="Get data sample",
         description="Returns a sample of rows from the CSV file")
async def get_sample(
    n: int = Query(10, description="Number of rows to sample", ge=1, le=1000),
    ddf=Depends(get_loaded_ddf)
):
    """Get a sample of rows from the CSV file"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Endpoint called: Get sample - Requested {n} rows")
    
    start_time = time.time()
    try:
        # Get the first n rows - this is optimized and doesn't load the whole dataset
        logger.info(f"[{request_id}] Retrieving {n} sample rows from Dask DataFrame")
        sample_data = ddf.head(n=n)
        
        # Convert to records
        logger.info(f"[{request_id}] Converting sample data to dictionary records")
        result = sample_data.to_dict('records')
        
        query_time = time.time() - start_time
        logger.info(f"[{request_id}] Sample request completed in {query_time:.4f} seconds")
        
        return {
            "count": len(result),
            "data": result,
            "query_time": query_time
        }
    except Exception as e:
        logger.error(f"[{request_id}] Error getting sample: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting sample: {str(e)}")

@app.get("/query", response_model=QueryResult, tags=["Data"], 
         summary="Query data with filters",
         description="Query the CSV data with column selection, filters, and pagination")
async def query_data(
    columns: Optional[str] = Query(None, description="Comma-separated list of columns to include (e.g., 'col1,col2,col3')"),
    filters: Optional[str] = Query(None, description="Filters in format column:value (comma-separated for multiple, e.g., 'col1:value1,col2:value2')"),
    limit: int = Query(100, description="Maximum number of rows to return", ge=1, le=10000),
    offset: int = Query(0, description="Number of rows to skip", ge=0),
    ddf=Depends(get_loaded_ddf)
):
    """Query the CSV data with filters and column selection"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Endpoint called: Query data")
    logger.info(f"[{request_id}] Query parameters: columns='{columns}', filters='{filters}', limit={limit}, offset={offset}")
    
    start_time = time.time()
    query_stages = {}
    
    try:
        # Start with the full DataFrame
        query_df = ddf
        stage_start = time.time()
        
        # Apply column selection if specified
        if columns:
            col_list = [col.strip() for col in columns.split(",")]
            # Validate columns
            invalid_cols = [col for col in col_list if col not in file_info["columns"]]
            if invalid_cols:
                logger.warning(f"[{request_id}] Invalid columns requested: {invalid_cols}")
                raise HTTPException(status_code=400, detail=f"Invalid columns: {invalid_cols}")
            
            logger.info(f"[{request_id}] Selecting columns: {col_list}")
            query_df = query_df[col_list]
        
        query_stages["column_selection"] = time.time() - stage_start
        stage_start = time.time()
        
        # Apply filters if specified
        if filters:
            filter_list = [f.strip() for f in filters.split(",")]
            logger.info(f"[{request_id}] Applying filters: {filter_list}")
            
            # OPTIMIZATION: Build a single mask for all filters
            # This is more efficient than applying filters one by one
            mask = None
            
            for filter_item in filter_list:
                if ":" not in filter_item:
                    logger.warning(f"[{request_id}] Invalid filter format: {filter_item}")
                    continue
                
                col, val = filter_item.split(":", 1)
                if col in file_info["columns"]:
                    logger.info(f"[{request_id}] Filtering where {col} = {val}")
                    current_mask = query_df[col] == val
                    
                    if mask is None:
                        mask = current_mask
                    else:
                        mask = mask & current_mask
                else:
                    logger.warning(f"[{request_id}] Filter uses unknown column: {col}")
            
            if mask is not None:
                query_df = query_df[mask]
        
        query_stages["filtering"] = time.time() - stage_start
        stage_start = time.time()
        
        # OPTIMIZATION: Apply pagination before computation
        logger.info(f"[{request_id}] Applying optimized pagination strategy")
        
        # Get total count estimate
        total_count = 0
        if file_info["row_count_computed"]:
            total_count = file_info["total_rows"]
            logger.info(f"[{request_id}] Using pre-computed row count: {total_count}")
        else:
            # If we don't have a count yet, use an approximate count
            logger.info(f"[{request_id}] Using approximate row count")
            total_count = query_df.npartitions * (query_df.npartitions * 100000)  # Rough estimate
        
        # OPTIMIZATION: Use parallel computation with map_partitions for better performance
        if offset > 0:
            # Skip partitions if possible
            rows_per_partition_estimate = total_count / query_df.npartitions
            partitions_to_skip = int(offset / rows_per_partition_estimate)
            
            if partitions_to_skip > 0 and partitions_to_skip < query_df.npartitions:
                logger.info(f"[{request_id}] Skipping approximately {partitions_to_skip} partitions")
                # This is a heuristic approach - not exact but much faster
                partition_offset = min(partitions_to_skip, query_df.npartitions - 1)
                # Get the remaining offset within the partition
                remaining_offset = offset - (partition_offset * rows_per_partition_estimate)
                remaining_offset = max(0, int(remaining_offset))
                
                # Read only the needed partitions
                logger.info(f"[{request_id}] Reading from partition {partition_offset} with remaining offset {remaining_offset}")
                
                # OPTIMIZATION: Use map_partitions for parallel computation
                if client:
                    logger.info(f"[{request_id}] Using parallel computation with distributed client")
                    # Define a function to apply to each partition
                    def process_partition(df, offset, limit):
                        if len(df) > offset:
                            return df.iloc[offset:offset+limit]
                        return df.iloc[0:0]  # Empty DataFrame with same structure
                    
                    # Apply to the first partition
                    first_part = query_df.partitions[partition_offset].map_partitions(
                        process_partition, remaining_offset, limit
                    )
                    
                    # If we need more rows, get from next partition
                    if limit > 0:
                        next_part = query_df.partitions[partition_offset+1].map_partitions(
                            process_partition, 0, limit
                        )
                        # Combine results
                        combined = dd.concat([first_part, next_part])
                        # Compute in parallel
                        result_df = combined.compute()
                        # Take only what we need
                        paginated_df = result_df.head(limit)
                    else:
                        # Just compute the first part
                        paginated_df = first_part.compute()
                else:
                    # Without distributed client, use the previous approach
                    result_df = query_df.partitions[partition_offset:partition_offset+2].compute()
                    if len(result_df) > remaining_offset:
                        paginated_df = result_df.iloc[remaining_offset:remaining_offset+limit]
                    else:
                        next_partition = min(partition_offset + 2, query_df.npartitions - 1)
                        result_df = query_df.partitions[next_partition:next_partition+1].compute()
                        paginated_df = result_df.iloc[:limit]
            else:
                # For small offsets, use optimized approach
                logger.info(f"[{request_id}] Using optimized approach for small offset")
                
                if client:
                    # With distributed client, use parallel computation
                    # Get more rows than needed to account for the offset
                    result_df = query_df.head(offset + limit * 2)  # Get extra rows to be safe
                    paginated_df = result_df.iloc[offset:offset+limit]
                else:
                    # Without client, use standard approach
                    result_df = query_df.head(offset + limit)
                    paginated_df = result_df.iloc[offset:offset+limit]
        else:
            # For queries starting at the beginning, just use head with parallel computation
            logger.info(f"[{request_id}] Using head strategy for pagination (no offset)")
            
            if client:
                # With distributed client, use optimized head
                future = client.submit(lambda df, n: df.head(n), query_df, limit)
                paginated_df = future.result()
            else:
                # Without client, use standard head
                paginated_df = query_df.head(limit).compute()
        
        query_stages["computation"] = time.time() - stage_start
        
        # Convert to list of dictionaries
        result = paginated_df.to_dict('records')
        
        query_time = time.time() - start_time
        
        # Log detailed timing information
        logger.info(f"[{request_id}] Query completed in {query_time:.4f} seconds")
        logger.info(f"[{request_id}] Query performance breakdown: {json.dumps(query_stages)}")
        logger.info(f"[{request_id}] Returned {len(result)} rows")
        
        return {
            "count": total_count,
            "data": result,
            "query_time": query_time
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Error querying data: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error querying data: {str(e)}")

@app.get("/stats/{column}", tags=["Analysis"], 
         summary="Get column statistics",
         description="Returns statistical information about a specific column")
async def get_column_stats(
    column: str = Path(..., description="Column name to get statistics for"),
    sample_size: float = Query(0.01, description="Fraction of data to sample for statistics (0.01 = 1%)", ge=0.001, le=0.5),
    ddf=Depends(get_loaded_ddf)
):
    """Get statistics for a specific column"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Endpoint called: Get column stats for '{column}' with sample_size={sample_size}")
    
    if column not in file_info["columns"]:
        logger.warning(f"[{request_id}] Requested stats for non-existent column: {column}")
        raise HTTPException(status_code=400, detail=f"Column '{column}' not found")
    
    start_time = time.time()
    try:
        # Check if column is numeric
        dtype = ddf[column].dtype
        is_numeric = pd.api.types.is_numeric_dtype(dtype)
        logger.info(f"[{request_id}] Column '{column}' has dtype {dtype} (numeric: {is_numeric})")
        
        result = {}
        
        # OPTIMIZATION: Always use sampling for better performance
        logger.info(f"[{request_id}] Sampling {sample_size*100}% of data for statistics")
        sample = ddf[column].sample(frac=sample_size).compute()
        
        # For numeric columns, compute statistics on the sample
        if is_numeric:
            logger.info(f"[{request_id}] Computing numeric statistics on sample")
            stats = sample.describe()
            result = stats.to_dict()
            result["sample_size"] = len(sample)
            result["sample_fraction"] = sample_size
            logger.info(f"[{request_id}] Numeric statistics computed on {len(sample)} rows")
        else:
            # For non-numeric columns, get unique values count from sample
            unique_vals = sample.value_counts().head(20).to_dict()
            result = {
                "unique_values_sample": unique_vals,
                "sample_size": len(sample),
                "sample_fraction": sample_size,
                "note": f"Statistics shown for {sample_size*100}% sample of data"
            }
            logger.info(f"[{request_id}] Sampled {len(sample)} values, found {len(unique_vals)} unique values in top 20")
        
        query_time = time.time() - start_time
        result["query_time"] = query_time
        
        logger.info(f"[{request_id}] Stats computation completed in {query_time:.4f} seconds")
        
        return result
    except Exception as e:
        logger.error(f"[{request_id}] Error getting column stats: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting column stats: {str(e)}")

@app.get("/column-info", response_model=Dict[str, Any], tags=["Information"],
         summary="Get detailed column information",
         description="Returns information about the columns in the CSV file, including original names and any renamed columns")
async def get_column_info(ddf=Depends(get_loaded_ddf)):
    """Get detailed information about the columns in the CSV file"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Endpoint called: Get column info")
    
    try:
        # Return information about the columns
        return {
            "original_columns": file_info["original_columns"],
            "current_columns": file_info["columns"],
            "renamed_columns": file_info["renamed_columns"],
            "column_mapping": {
                i: {
                    "original_name": file_info["original_columns"][i],
                    "current_name": file_info["columns"][i]
                } for i in range(len(file_info["original_columns"]))
            }
        }
    except Exception as e:
        logger.error(f"[{request_id}] Error getting column info: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting column info: {str(e)}")

@app.get("/account-search", response_model=QueryResult, tags=["Data"],
         summary="Search for transactions by account number",
         description="Search for transactions where either the From Account or To Account matches the provided account number")
async def search_by_account(
    account: str = Query(..., description="Account number to search for"),
    limit: int = Query(100, description="Maximum number of rows to return", ge=1, le=10000),
    offset: int = Query(0, description="Number of rows to skip", ge=0),
    ddf=Depends(get_loaded_ddf)
):
    """Search for transactions by account number in either From Account or To Account"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Endpoint called: Search by account")
    logger.info(f"[{request_id}] Query parameters: account='{account}', limit={limit}, offset={offset}")
    
    start_time = time.time()
    query_stages = {}
    
    try:
        # Start with the full DataFrame
        query_df = ddf
        stage_start = time.time()
        
        # Find the renamed column names for "Account" if they exist
        from_account_col = "Account"
        to_account_col = "Account"
        
        # Check if we have renamed columns
        for i, col in enumerate(file_info["original_columns"]):
            if col == "Account":
                # Get the current name for this column
                current_name = file_info["columns"][i]
                
                # Determine if this is the From Account or To Account based on position
                prev_col = file_info["original_columns"][i-1] if i > 0 else ""
                
                if prev_col == "From Bank":
                    from_account_col = current_name
                    logger.info(f"[{request_id}] Identified From Account column as '{from_account_col}'")
                elif prev_col == "To Bank":
                    to_account_col = current_name
                    logger.info(f"[{request_id}] Identified To Account column as '{to_account_col}'")
        
        # Create a mask for accounts in either column
        logger.info(f"[{request_id}] Searching for account '{account}' in columns '{from_account_col}' and '{to_account_col}'")
        from_mask = query_df[from_account_col] == account
        to_mask = query_df[to_account_col] == account
        
        # Combine masks with OR
        mask = from_mask | to_mask
        query_df = query_df[mask]
        
        query_stages["filtering"] = time.time() - stage_start
        stage_start = time.time()
        
        # Get total count estimate for the filtered data
        # This is an approximation to avoid counting the entire filtered dataset
        total_count_estimate = query_df.npartitions * 1000  # Rough estimate
        
        # Apply pagination with optimized strategy
        logger.info(f"[{request_id}] Applying optimized pagination strategy")
        
        if client:
            # With distributed client, use parallel computation
            logger.info(f"[{request_id}] Using parallel computation with distributed client")
            future = client.submit(lambda df, n: df.head(offset + limit), query_df, offset + limit)
            result_df = future.result()
            paginated_df = result_df.iloc[offset:offset+limit] if len(result_df) > offset else result_df.iloc[:0]
        else:
            # Without client, use standard approach
            result_df = query_df.head(offset + limit)
            paginated_df = result_df.iloc[offset:offset+limit] if len(result_df) > offset else result_df.iloc[:0]
        
        query_stages["computation"] = time.time() - stage_start
        
        # Convert to list of dictionaries
        result = paginated_df.to_dict('records')
        
        query_time = time.time() - start_time
        
        # Log detailed timing information
        logger.info(f"[{request_id}] Query completed in {query_time:.4f} seconds")
        logger.info(f"[{request_id}] Query performance breakdown: {json.dumps(query_stages)}")
        logger.info(f"[{request_id}] Returned {len(result)} rows")
        
        return {
            "count": total_count_estimate,
            "data": result,
            "query_time": query_time
        }
    except Exception as e:
        logger.error(f"[{request_id}] Error searching by account: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error searching by account: {str(e)}")

@app.get("/rows/{start_idx}", tags=["Data"],
         summary="Get specific rows by index",
         description="Get a range of rows by their index position")
async def get_rows_by_index(
    start_idx: int = Path(..., description="Starting index (0-based)", ge=0),
    count: int = Query(10, description="Number of rows to return", ge=1, le=1000),
    ddf=Depends(get_loaded_ddf)
):
    """Get specific rows by their index"""
    request_id = str(uuid.uuid4())
    logger.info(f"[{request_id}] Endpoint called: Get rows by index - start={start_idx}, count={count}")
    
    start_time = time.time()
    try:
        # Estimate which partition contains the requested rows
        if not file_info["row_count_computed"]:
            logger.info(f"[{request_id}] Row count not computed yet, using partition-based approach")
            # Use a partition-based approach if we don't have the total count
            rows_per_partition_estimate = 100000  # Rough estimate
            partition_idx = int(start_idx / rows_per_partition_estimate)
            partition_idx = min(partition_idx, ddf.npartitions - 1)
            
            logger.info(f"[{request_id}] Estimated partition: {partition_idx}")
            
            # Read the partition and a bit more to ensure we have enough rows
            partition_data = ddf.partitions[partition_idx:partition_idx+2].compute()
            
            # Calculate the offset within the partition
            partition_offset = start_idx % rows_per_partition_estimate
            
            if len(partition_data) > partition_offset:
                result_data = partition_data.iloc[partition_offset:partition_offset+count]
            else:
                # If we don't have enough rows, return what we have
                result_data = partition_data.iloc[:count]
        else:
            # If we know the total count, we can be more precise
            total_rows = file_info["total_rows"]
            
            if start_idx >= total_rows:
                logger.warning(f"[{request_id}] Requested start index {start_idx} exceeds total rows {total_rows}")
                raise HTTPException(status_code=400, detail=f"Start index {start_idx} exceeds total row count {total_rows}")
            
            # Use the optimized approach to get specific rows
            rows_per_partition = total_rows / ddf.npartitions
            partition_idx = int(start_idx / rows_per_partition)
            partition_idx = min(partition_idx, ddf.npartitions - 1)
            
            logger.info(f"[{request_id}] Calculated partition: {partition_idx} based on {total_rows} total rows")
            
            # Read the partition
            partition_data = ddf.partitions[partition_idx].compute()
            
            # Calculate the offset within the partition
            partition_offset = start_idx - (partition_idx * rows_per_partition)
            partition_offset = max(0, int(partition_offset))
            
            if len(partition_data) > partition_offset:
                if partition_offset + count <= len(partition_data):
                    # If we have enough rows in this partition
                    result_data = partition_data.iloc[partition_offset:partition_offset+count]
                else:
                    # If we need to read from the next partition too
                    rows_from_current = len(partition_data) - partition_offset
                    rows_needed_from_next = count - rows_from_current
                    
                    logger.info(f"[{request_id}] Need to read from next partition: {rows_from_current} from current, {rows_needed_from_next} from next")
                    
                    # Get rows from current partition
                    current_rows = partition_data.iloc[partition_offset:]
                    
                    # Get rows from next partition if it exists
                    if partition_idx + 1 < ddf.npartitions:
                        next_partition_data = ddf.partitions[partition_idx + 1].compute()
                        next_rows = next_partition_data.iloc[:rows_needed_from_next]
                        
                        # Combine the results
                        result_data = pd.concat([current_rows, next_rows])
                    else:
                        # If there's no next partition, return what we have
                        result_data = current_rows
            else:
                # This shouldn't happen with correct calculations, but just in case
                result_data = partition_data.head(count)
        
        # Convert to records
        result = result_data.to_dict('records')
        query_time = time.time() - start_time
        
        logger.info(f"[{request_id}] Retrieved {len(result)} rows in {query_time:.4f} seconds")
        
        return {
            "count": len(result),
            "data": result,
            "query_time": query_time
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Error getting rows by index: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Error getting rows: {str(e)}")

# Custom OpenAPI schema with more metadata
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="Large CSV Query API",
        version="1.0.0",
        description="API for querying and analyzing large CSV files using Dask",
        routes=app.routes,
    )
    
    # Add additional metadata
    openapi_schema["info"]["x-logo"] = {
        "url": "https://fastapi.tiangolo.com/img/logo-margin/logo-teal.png"
    }
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info(f"Starting FastAPI server at {datetime.now().isoformat()}")
    logger.info(f"CSV file to be loaded: {file_info['file_path']}")
    logger.info("=" * 50)
    print("Starting FastAPI server for CSV querying...")
    print("Swagger UI will be available at: http://localhost:8000/docs")
    print("ReDoc UI will be available at: http://localhost:8000/redoc")
    print("Detailed logs will be written to api_server.log")
    uvicorn.run("load_large_csv_with_dask:app", host="0.0.0.0", port=8000, reload=True)
