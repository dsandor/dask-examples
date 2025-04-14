# Dask S3 Equity Data Processor

A Python script that uses Dask with AWS Fargate to process and query large equity datasets stored in S3. The script identifies and joins CSV files from multiple folders, handling duplicate columns intelligently.

## Features

- **AWS Fargate Integration**: Uses the `foo-cluster` Fargate cluster for distributed processing
- **S3 Data Discovery**: Automatically finds folders containing 'EQUITY' in their names
- **Smart File Selection**: Selects the most recent CSV file from each folder based on date in filename
- **Intelligent Joining**: Joins data using ID_BB_GLOBAL as the primary key
- **Duplicate Column Handling**: Resolves duplicate columns by using data from the most recent file
- **Distributed Processing**: Leverages Dask for parallel data processing across multiple nodes

## Prerequisites

- AWS credentials configured with access to S3 and Fargate
- Python 3.7+
- Required packages (install using `pip install -r requirements.txt`):
  - dask
  - pandas
  - boto3
  - s3fs
  - dask-cloudprovider

## Configuration

Set the following environment variables before running the script:

```bash
# Required
export S3_BUCKET=your-s3-bucket-name

# Optional (defaults shown)
export AWS_REGION=us-east-1
export S3_ROOT_FOLDER=  # Root folder to start search from (empty for bucket root)
```

## Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run the script with default settings
python dask_s3_equity_processor.py

# Or specify options via command line arguments
python dask_s3_equity_processor.py --bucket your-bucket --root-folder data/equity --region us-west-2 --cluster-name custom-cluster
```

## Command Line Arguments

The script supports the following command line arguments:

- `--bucket`: S3 bucket name (overrides S3_BUCKET environment variable)
- `--region`: AWS region (overrides AWS_REGION environment variable)
- `--root-folder`: Root folder in S3 bucket to start search from (overrides S3_ROOT_FOLDER environment variable)
- `--cluster-name`: Fargate cluster name (overrides default cluster name)

## How It Works

1. **Cluster Setup**: Creates a Dask cluster on AWS Fargate named `foo-cluster`
2. **Folder Discovery**: Finds all folders in the S3 bucket containing 'EQUITY' in their names
3. **File Selection**: For each folder, identifies the most recent CSV file based on date in filename (YYYYMMDD format)
4. **Data Loading**: Loads each CSV file into a Dask DataFrame
5. **Data Merging**: Joins all DataFrames using ID_BB_GLOBAL as the primary key
6. **Duplicate Resolution**: When duplicate columns exist, uses data from the most recent file
7. **Result**: Creates a single wide Dask DataFrame ready for querying

## Extending the Script

### Adding Custom Queries

You can extend the script to perform custom queries on the merged DataFrame:

```python
def main():
    # ... existing code ...
    
    # Get the merged DataFrame
    merged_df, client, cluster = process_equity_data()
    
    # Perform custom queries
    result = merged_df[merged_df.COLUMN_NAME > threshold].compute()
    
    # ... process results ...
```

### Saving Results

To save the processed data:

```python
# Save to CSV
merged_df.to_csv('s3://output-bucket/processed_data/*.csv')

# Save to Parquet (recommended for large datasets)
merged_df.to_parquet('s3://output-bucket/processed_data/')
```

## Troubleshooting

### Common Issues

1. **AWS Credentials**: Ensure your AWS credentials are properly configured with access to S3 and Fargate
2. **Memory Issues**: If you encounter memory errors, try reducing the worker memory or increasing the number of partitions
3. **Timeout Errors**: For large datasets, increase the `find_address_timeout` parameter in the FargateCluster configuration

### Logs

The script logs detailed information to both the console and a log file (`dask_s3_equity_processor.log`). Check these logs for troubleshooting.

## Performance Optimization

- **Adjust Worker Resources**: Modify the `worker_cpu` and `worker_mem` parameters in the `setup_fargate_cluster` function
- **Tune Blocksize**: Adjust the `blocksize` parameter in `read_csv` based on your file sizes
- **Partition Count**: For very large datasets, you may need to repartition: `merged_df = merged_df.repartition(npartitions=100)`
