# Azure Data Factory SFTP to Blob Storage Example

This Terraform example demonstrates how to create an Azure Data Factory pipeline that transfers files from an SFTP server to Azure Blob Storage.

## Architecture

This example creates:

1. A new resource group named `adf-example-rg`
2. An Azure Key Vault with fake SFTP credentials
3. An Azure Storage Account with a container named `adf-example-bucket`
4. An Azure Data Factory with:
   - SFTP linked service pointing to `fake-sftp.example.com`
   - Key Vault linked service for secure credential management
   - Blob Storage linked service
   - A pipeline that copies files from SFTP to Blob Storage
   - A daily trigger to run the pipeline

## Prerequisites

- Azure CLI installed and configured
- Terraform installed
- Azure subscription with appropriate permissions

## Deployment

1. Initialize Terraform:
   ```bash
   terraform init
   ```

2. Review the deployment plan:
   ```bash
   terraform plan
   ```

3. Apply the configuration:
   ```bash
   terraform apply
   ```

4. To destroy the resources when done:
   ```bash
   terraform destroy
   ```

## Configuration

You can customize the deployment by modifying the variables in `variables.tf` or by creating a `terraform.tfvars` file with your own values.

Key variables include:
- `location`: Azure region for deployment (default: "eastus")
- `sftp_host`: SFTP server hostname (default: "fake-sftp.example.com")
- `sftp_path`: Path on SFTP server to monitor (default: "/upload/")
- `sftp_file_pattern`: File pattern to copy (default: "*.csv")
- `storage_container_name`: Name of the storage container (default: "adf-example-bucket")

## Security Notes

- This example stores fake SFTP credentials in Key Vault
- In a production environment, use Azure Key Vault to store real credentials
- The Key Vault is configured with access policies for the current user
- For production use, consider adding additional security measures like private endpoints

## Idempotency

This Terraform configuration is idempotent:
- It will create resources if they don't exist
- It will update resources if they exist but have different configurations
- It will leave resources unchanged if they exist with the same configuration
- Random string generation ensures unique resource names for each deployment

## Pipeline Details

The pipeline is configured to:
1. Scan the SFTP server directory `/upload/` for CSV files
2. Copy those files to the Azure Blob Storage container `adf-example-bucket` in the path `sftp-data/`
3. Run daily at midnight UTC

## Monitoring and Management

After deployment, you can:
- Monitor pipeline runs in the Azure Portal
- View copied files in the Azure Storage Explorer
- Manage credentials in the Azure Key Vault
