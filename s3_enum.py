import boto3
import re
import json
from datetime import datetime
from pathlib import Path
import logging
from typing import List, Dict, Optional
import argparse
import os
import botocore

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Enable botocore debug logging to see the actual API endpoints
logging.getLogger('botocore').setLevel(logging.DEBUG)
logging.getLogger('botocore.auth').setLevel(logging.DEBUG)
logging.getLogger('botocore.endpoint').setLevel(logging.DEBUG)

class S3Enumerator:
    def __init__(self, bucket_name: str, root_path: str = "", include_pattern: Optional[str] = None, 
                 exclude_pattern: Optional[str] = None, download: bool = False, 
                 download_dir: Optional[str] = None):
        self.session = boto3.Session()
        self.s3_client = self.session.client('s3')
        self.bucket_name = bucket_name
        self.root_path = root_path.rstrip('/')  # Remove trailing slash if present
        self.include_pattern = re.compile(include_pattern) if include_pattern else None
        self.exclude_pattern = re.compile(exclude_pattern) if exclude_pattern else None
        self.download = download
        self.download_dir = download_dir
        self.total_size = 0
        self.latest_files = []
        
        # Print credential diagnostics
        self._print_credential_diagnostics()

    def _print_credential_diagnostics(self):
        """Print diagnostic information about AWS credentials."""
        try:
            credentials = self.session.get_credentials()
            if credentials:
                logger.info(f"Using AWS Access Key ID: {credentials.access_key[:5]}...{credentials.access_key[-5:]}")
            
            sts_client = self.session.client('sts')
            identity = sts_client.get_caller_identity()
            logger.info(f"AWS Caller Identity ARN: {identity['Arn']}")
            logger.info(f"AWS Account: {identity['Account']}")
            logger.info(f"AWS User ID: {identity['UserId']}")
        except Exception as e:
            logger.error(f"Error getting credential diagnostics: {str(e)}")

    def _log_403_error(self, error: botocore.exceptions.ClientError, operation: str):
        """Log detailed information about a 403 error."""
        logger.error(f"Access Denied (403) during {operation}")
        logger.error(f"Full error response: {json.dumps(error.response, indent=2)}")
        
        # Log the SDK URL if available
        if hasattr(error, 'response') and 'ResponseMetadata' in error.response:
            metadata = error.response['ResponseMetadata']
            if 'HTTPStatusCode' in metadata:
                logger.error(f"HTTP Status Code: {metadata['HTTPStatusCode']}")
            if 'RequestId' in metadata:
                logger.error(f"Request ID: {metadata['RequestId']}")
            if 'HTTPHeaders' in metadata:
                logger.error(f"HTTP Headers: {json.dumps(metadata['HTTPHeaders'], indent=2)}")
        
        # Print additional diagnostics for 403 errors
        self._print_credential_diagnostics()

    def should_process_directory(self, directory: str) -> bool:
        """Check if directory should be processed based on include/exclude patterns."""
        if self.exclude_pattern and self.exclude_pattern.search(directory):
            return False
        if self.include_pattern and not self.include_pattern.search(directory):
            return False
        return True

    def download_file(self, s3_key: str, local_path: str) -> bool:
        """Download a file from S3 to the local filesystem."""
        try:
            # Create the directory structure if it doesn't exist
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            
            # Download the file
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            logger.info(f"Downloaded file to: {local_path}")
            return True
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '403':
                self._log_403_error(e, f"downloading {s3_key}")
            else:
                logger.error(f"Error downloading file {s3_key}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error downloading file {s3_key}: {str(e)}")
            return False

    def file_exists_locally(self, local_path: str) -> bool:
        """Check if a file already exists in the local filesystem."""
        return os.path.exists(local_path)

    def get_recent_csv_gz_files(self, prefix: str) -> List[Optional[Dict]]:
        """Get the two most recent CSV.GZ files in the given prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=1000
            )

            if 'Contents' not in response:
                return [None, None]

            # Filter for CSV.GZ files
            csv_files = [
                obj for obj in response['Contents']
                if obj['Key'].endswith('.csv.gz')
            ]

            if not csv_files:
                return [None, None]

            # Sort by last modified date, most recent first
            sorted_files = sorted(csv_files, key=lambda x: x['LastModified'], reverse=True)
            recent_files = sorted_files[:2]  # Get the two most recent files
            
            result = []
            for file in recent_files:
                file_info = {
                    'path': f"s3://{self.bucket_name}/{file['Key']}",
                    'size': file['Size'],
                    'last_modified': file['LastModified'].isoformat(),
                    's3_key': file['Key']
                }

                # Download the file if requested
                if self.download and self.download_dir:
                    # Remove root path from the key to maintain relative structure
                    relative_key = file['Key']
                    if self.root_path:
                        relative_key = relative_key[len(self.root_path):].lstrip('/')
                    
                    # Construct local path
                    local_path = os.path.join(self.download_dir, relative_key)
                    file_info['local_path'] = local_path
                    
                    # Only download if file doesn't exist locally
                    if not self.file_exists_locally(local_path):
                        if self.download_file(file['Key'], local_path):
                            logger.info(f"Downloaded new file: {local_path}")
                        else:
                            logger.warning(f"Failed to download file: {local_path}")
                    else:
                        logger.info(f"File already exists locally: {local_path}")

                result.append(file_info)

            # Pad with None if we have fewer than 2 files
            while len(result) < 2:
                result.append(None)

            return result

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '403':
                self._log_403_error(e, f"listing objects in {prefix}")
            else:
                logger.error(f"Error processing prefix {prefix}: {str(e)}")
            return [None, None]
        except Exception as e:
            logger.error(f"Error processing prefix {prefix}: {str(e)}")
            return [None, None]

    def enumerate_directories(self, prefix: str = "") -> None:
        """Recursively enumerate directories and process CSV.GZ files."""
        try:
            # Construct the full prefix including root path
            full_prefix = f"{self.root_path}/{prefix}".lstrip('/')
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            # Get all objects with the given prefix
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=full_prefix, Delimiter='/'):
                # Process directories
                if 'CommonPrefixes' in page:
                    for prefix_obj in page['CommonPrefixes']:
                        directory = prefix_obj['Prefix']
                        # Remove root path from directory for pattern matching
                        relative_dir = directory[len(self.root_path):].lstrip('/') if self.root_path else directory
                        if self.should_process_directory(relative_dir):
                            logger.info(f"Processing directory: {directory}")
                            self.enumerate_directories(relative_dir)

                # Process files in current directory
                if 'Contents' in page:
                    recent_files = self.get_recent_csv_gz_files(full_prefix)
                    for file_info in recent_files:
                        if file_info:
                            self.latest_files.append(file_info)
                            self.total_size += file_info['size']
                            logger.info(f"Found file: {file_info['path']} (Size: {file_info['size']} bytes)")

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '403':
                self._log_403_error(e, "enumerating directories")
            else:
                logger.error(f"Error enumerating directories: {str(e)}")
        except Exception as e:
            logger.error(f"Error enumerating directories: {str(e)}")

    def save_results(self, output_file: str) -> None:
        """Save results to a JSON file."""
        results = {
            'bucket': self.bucket_name,
            'root_path': self.root_path,
            'total_size': self.total_size,
            'latest_files': self.latest_files
        }
        
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"Results saved to {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Enumerate S3 directories and process CSV.GZ files')
    parser.add_argument('bucket', help='S3 bucket name')
    parser.add_argument('--root-path', default='', help='Root path within the bucket to start enumeration')
    parser.add_argument('--include', help='Regex pattern to include directories')
    parser.add_argument('--exclude', help='Regex pattern to exclude directories')
    parser.add_argument('--output', default='s3_enum_results.json', help='Output JSON file path')
    parser.add_argument('--download', action='store_true', help='Download the latest files')
    parser.add_argument('--download-dir', help='Directory to download files to')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging for botocore')
    
    args = parser.parse_args()

    # Validate download arguments
    if args.download and not args.download_dir:
        parser.error("--download-dir is required when --download is specified")

    # Enable debug logging if requested
    if args.debug:
        logging.getLogger('botocore').setLevel(logging.DEBUG)
        logging.getLogger('botocore.auth').setLevel(logging.DEBUG)
        logging.getLogger('botocore.endpoint').setLevel(logging.DEBUG)

    enumerator = S3Enumerator(
        args.bucket, 
        args.root_path, 
        args.include, 
        args.exclude,
        args.download,
        args.download_dir
    )
    enumerator.enumerate_directories()
    
    logger.info(f"Total size of latest files: {enumerator.total_size} bytes")
    enumerator.save_results(args.output)

if __name__ == "__main__":
    main() 