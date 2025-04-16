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
                logger.error(f"Access Denied (403) when downloading {s3_key}")
                logger.error(f"Full error response: {json.dumps(e.response, indent=2)}")
                # Print additional diagnostics for 403 errors
                self._print_credential_diagnostics()
            else:
                logger.error(f"Error downloading file {s3_key}: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error downloading file {s3_key}: {str(e)}")
            return False

    def get_latest_csv_gz(self, prefix: str) -> Optional[Dict]:
        """Get the most recent CSV.GZ file in the given prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                MaxKeys=1000
            )

            if 'Contents' not in response:
                return None

            # Filter for CSV.GZ files and find the most recent one
            csv_files = [
                obj for obj in response['Contents']
                if obj['Key'].endswith('.csv.gz')
            ]

            if not csv_files:
                return None

            # Sort by last modified date, most recent first
            latest_file = max(csv_files, key=lambda x: x['LastModified'])
            
            file_info = {
                'path': f"s3://{self.bucket_name}/{latest_file['Key']}",
                'size': latest_file['Size'],
                'last_modified': latest_file['LastModified'].isoformat(),
                's3_key': latest_file['Key']
            }

            # Download the file if requested
            if self.download and self.download_dir:
                # Remove root path from the key to maintain relative structure
                relative_key = latest_file['Key']
                if self.root_path:
                    relative_key = relative_key[len(self.root_path):].lstrip('/')
                
                # Construct local path
                local_path = os.path.join(self.download_dir, relative_key)
                file_info['local_path'] = local_path
                self.download_file(latest_file['Key'], local_path)

            return file_info

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '403':
                logger.error(f"Access Denied (403) when listing objects in {prefix}")
                logger.error(f"Full error response: {json.dumps(e.response, indent=2)}")
                # Print additional diagnostics for 403 errors
                self._print_credential_diagnostics()
            else:
                logger.error(f"Error processing prefix {prefix}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Error processing prefix {prefix}: {str(e)}")
            return None

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
                    latest_file = self.get_latest_csv_gz(full_prefix)
                    if latest_file:
                        self.latest_files.append(latest_file)
                        self.total_size += latest_file['size']
                        logger.info(f"Found latest file: {latest_file['path']} (Size: {latest_file['size']} bytes)")

        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '403':
                logger.error(f"Access Denied (403) when enumerating directories")
                logger.error(f"Full error response: {json.dumps(e.response, indent=2)}")
                # Print additional diagnostics for 403 errors
                self._print_credential_diagnostics()
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
    
    args = parser.parse_args()

    # Validate download arguments
    if args.download and not args.download_dir:
        parser.error("--download-dir is required when --download is specified")

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