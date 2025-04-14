#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script to process large equity datasets from S3 using Dask on AWS Fargate.

This script:
1. Connects to S3 and finds folders containing 'EQUITY' in their names
2. Identifies the most recent CSV file in each folder (based on YYYYMMDD in filename)
3. Loads these files into Dask dataframes
4. Joins them using ID_BB_GLOBAL as the primary key
5. Handles duplicate columns by using data from the most recent file
6. Creates a Dask cluster on AWS Fargate for distributed processing
"""

import os
import re
import boto3
import logging
import pandas as pd
import dask.dataframe as dd
from datetime import datetime
from dask.distributed import Client
from dask_cloudprovider.aws import FargateCluster

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("dask_s3_equity_processor.log")
    ]
)
logger = logging.getLogger("dask_s3_equity_processor")

# AWS Configuration
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
S3_BUCKET = os.environ.get('S3_BUCKET')
FARGATE_CLUSTER_NAME = 'foo-cluster'

# Regular expression to extract date from filename (YYYYMMDD)
DATE_PATTERN = re.compile(r'(\d{8})')

def setup_fargate_cluster():
    """
    Set up a Dask cluster on AWS Fargate.
    """
    logger.info(f"Setting up Dask cluster on AWS Fargate: {FARGATE_CLUSTER_NAME}")
    
    try:
        # Create a Fargate cluster
        cluster = FargateCluster(
            cluster_name=FARGATE_CLUSTER_NAME,
            n_workers=4,  # Adjust based on your needs
            worker_cpu=1024,  # 1 vCPU
            worker_mem=4096,  # 4 GB
            scheduler_cpu=1024,  # 1 vCPU
            scheduler_mem=4096,  # 4 GB
            image="daskdev/dask:latest",  # Use the official Dask image
            region=AWS_REGION,
            fargate_use_private_ip=False,
            security_groups=None,  # Specify if needed
            skip_cleanup=False,
            find_address_timeout=60,
            environment={
                "AWS_REGION": AWS_REGION
            }
        )
        
        # Create a Dask client
        client = Client(cluster)
        logger.info(f"Dask cluster dashboard available at: {client.dashboard_link}")
        
        return client, cluster
    
    except Exception as e:
        logger.error(f"Error setting up Fargate cluster: {str(e)}")
        raise

def find_equity_folders(s3_client, bucket):
    """
    Find all folders in S3 bucket that contain 'EQUITY' in their name.
    
    Args:
        s3_client: Boto3 S3 client
        bucket: S3 bucket name
        
    Returns:
        List of folder paths
    """
    logger.info(f"Finding equity folders in bucket: {bucket}")
    
    # List all objects in the bucket
    paginator = s3_client.get_paginator('list_objects_v2')
    
    equity_folders = set()
    
    # Paginate through results
    for page in paginator.paginate(Bucket=bucket):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                
                # Extract folder path
                folder_path = os.path.dirname(key)
                
                # Check if folder contains 'EQUITY'
                if 'EQUITY' in folder_path.upper():
                    equity_folders.add(folder_path)
    
    logger.info(f"Found {len(equity_folders)} equity folders")
    return list(equity_folders)

def get_most_recent_csv(s3_client, bucket, folder):
    """
    Find the most recent CSV file in a folder based on the date in the filename.
    
    Args:
        s3_client: Boto3 S3 client
        bucket: S3 bucket name
        folder: Folder path in S3
        
    Returns:
        Path to the most recent CSV file
    """
    logger.info(f"Finding most recent CSV in folder: {folder}")
    
    # List all objects in the folder
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=folder + '/'
    )
    
    csv_files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            
            # Check if file is a gzipped CSV
            if key.endswith('.csv.gz'):
                # Extract date from filename
                date_match = DATE_PATTERN.search(os.path.basename(key))
                
                if date_match:
                    date_str = date_match.group(1)
                    date = datetime.strptime(date_str, '%Y%m%d')
                    
                    csv_files.append({
                        'path': key,
                        'date': date,
                        'date_str': date_str
                    })
    
    if not csv_files:
        logger.warning(f"No CSV files found in folder: {folder}")
        return None
    
    # Sort by date (newest first)
    csv_files.sort(key=lambda x: x['date'], reverse=True)
    
    most_recent = csv_files[0]
    logger.info(f"Most recent CSV in {folder}: {most_recent['path']} (date: {most_recent['date_str']})")
    
    return most_recent

def load_csv_to_dask(s3_path, date_str):
    """
    Load a CSV file from S3 into a Dask DataFrame.
    
    Args:
        s3_path: S3 path to the CSV file
        date_str: Date string from the filename
        
    Returns:
        Dask DataFrame with the CSV data
    """
    logger.info(f"Loading CSV from S3: {s3_path}")
    
    # Construct the full S3 path
    full_s3_path = f"s3://{S3_BUCKET}/{s3_path}"
    
    try:
        # Load the CSV file into a Dask DataFrame
        df = dd.read_csv(
            full_s3_path,
            compression='gzip',
            blocksize='64MB',  # Adjust based on your file size
            assume_missing=True,  # Handle missing values
            storage_options={
                'region_name': AWS_REGION
            }
        )
        
        # Add a column to track the source file date
        df['_source_date'] = date_str
        
        return df
    
    except Exception as e:
        logger.error(f"Error loading CSV {s3_path}: {str(e)}")
        return None

def merge_dataframes(dfs, date_info):
    """
    Merge multiple Dask DataFrames using ID_BB_GLOBAL as the primary key.
    Handle duplicate columns by using data from the most recent file.
    
    Args:
        dfs: List of Dask DataFrames
        date_info: Dictionary mapping DataFrame index to date string
        
    Returns:
        Merged Dask DataFrame
    """
    if not dfs:
        logger.error("No DataFrames to merge")
        return None
    
    logger.info(f"Merging {len(dfs)} DataFrames")
    
    # Sort DataFrames by date (newest first)
    sorted_indices = sorted(date_info.keys(), key=lambda i: date_info[i], reverse=True)
    sorted_dfs = [dfs[i] for i in sorted_indices]
    
    # Start with the most recent DataFrame
    result = sorted_dfs[0]
    
    # Track columns we've already seen
    seen_columns = set(result.columns)
    
    # Merge with remaining DataFrames
    for i, df in enumerate(sorted_dfs[1:], 1):
        logger.info(f"Merging DataFrame {i} of {len(sorted_dfs)-1}")
        
        # Identify columns to keep (exclude duplicates we've already seen)
        columns_to_keep = ['ID_BB_GLOBAL']  # Always keep the join key
        
        for col in df.columns:
            if col not in seen_columns:
                columns_to_keep.append(col)
                seen_columns.add(col)
        
        # Skip if there are no new columns to add
        if len(columns_to_keep) <= 1:
            logger.info(f"Skipping DataFrame {i} as it has no new columns")
            continue
        
        # Select only the columns we want to keep
        df_subset = df[columns_to_keep]
        
        # Merge with the result
        result = result.merge(
            df_subset,
            on='ID_BB_GLOBAL',
            how='outer'  # Use outer join to keep all rows
        )
    
    return result

def main():
    """
    Main function to process equity data from S3 using Dask on AWS Fargate.
    """
    logger.info("Starting equity data processing with Dask on AWS Fargate")
    
    if not S3_BUCKET:
        logger.error("S3_BUCKET environment variable not set")
        return
    
    try:
        # Set up AWS clients
        s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # Set up Dask cluster on Fargate
        client, cluster = setup_fargate_cluster()
        
        # Find equity folders
        equity_folders = find_equity_folders(s3_client, S3_BUCKET)
        
        if not equity_folders:
            logger.error("No equity folders found")
            return
        
        # Get most recent CSV from each folder
        csv_files = []
        
        for folder in equity_folders:
            csv_info = get_most_recent_csv(s3_client, S3_BUCKET, folder)
            
            if csv_info:
                csv_files.append(csv_info)
        
        if not csv_files:
            logger.error("No CSV files found")
            return
        
        # Load CSV files into Dask DataFrames
        dfs = []
        date_info = {}
        
        for i, csv_info in enumerate(csv_files):
            df = load_csv_to_dask(csv_info['path'], csv_info['date_str'])
            
            if df is not None:
                dfs.append(df)
                date_info[len(dfs) - 1] = csv_info['date']
        
        if not dfs:
            logger.error("No DataFrames loaded")
            return
        
        # Merge DataFrames
        merged_df = merge_dataframes(dfs, date_info)
        
        if merged_df is None:
            logger.error("Failed to merge DataFrames")
            return
        
        # Print information about the merged DataFrame
        logger.info(f"Merged DataFrame has {len(merged_df.columns)} columns")
        logger.info(f"Column names: {list(merged_df.columns)}")
        
        # Compute basic statistics (this will trigger actual computation)
        logger.info("Computing DataFrame statistics...")
        stats = client.compute(merged_df.describe())
        stats = stats.result()
        
        logger.info("Processing complete")
        
        # Return the merged DataFrame for further processing
        return merged_df, client, cluster
    
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        raise
    
    finally:
        logger.info("Cleaning up resources")
        # Note: We don't close the client or cluster here to allow for further processing

if __name__ == "__main__":
    main()
