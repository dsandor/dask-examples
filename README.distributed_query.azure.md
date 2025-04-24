# Distributed Query System Deployment on Azure

## Overview

This document describes the deployment of a distributed query system on Azure using Terraform. The system consists of a data server and multiple distributed query servers, all deployed in Azure Container Apps with Azure Blob Storage for data files.

## Architecture

The distributed query system includes:

1. **Data Server**: Main server that handles data storage and processing
2. **Query Servers**: One server per data table, each processing queries for specific datasets
3. **Azure Blob Storage**: Stores data files and configuration
4. **Azure Container Apps**: Hosts all containerized services with 8GB RAM allocation each

## Components

### Infrastructure (Azure Resources)

- **Resource Group**: Contains all deployed resources
- **Storage Account**: Provides blob storage for data files
- **Container Registry**: Stores Docker images for the services
- **Container App Environment**: Hosts all container instances

### Application Components

- **Data Server Container**: Based on `Dockerfile`, handles data storage and processing
- **Query Server Containers**: Based on `Dockerfile`, one per table defined in config.json
- **Distributed Query Server**: Based on `Dockerfile.distributed`, coordinates queries across servers

## Terraform Configuration

The Terraform configuration consists of:

- **main.tf**: Main infrastructure definition
- **variables.tf**: Customizable deployment parameters
- **outputs.tf**: Information provided after deployment
- **build_and_push.sh**: Script to build and push Docker images
- **azure_config.json.tpl**: Template for Azure-specific configuration

## Deployment Instructions

### Prerequisites

- Azure CLI installed and configured
- Terraform installed
- Docker installed
- Azure subscription

### Deployment Steps

1. **Prepare Environment**:
   ```bash
   cd terraform
   chmod +x build_and_push.sh
   ```

2. **Build and Push Docker Images**:
   ```bash
   ./build_and_push.sh
   ```

3. **Initialize Terraform**:
   ```bash
   terraform init
   ```

4. **Deploy Infrastructure**:
   ```bash
   terraform apply
   ```

## Configuration Details

The system uses `config.json` to determine:

- Number of query servers to deploy (one per table)
- Data files to upload to Azure Blob Storage
- Configuration for each query server

Example config.json:
```json
{
    "tables": {
        "labels": {
            "url": "http://192.168.1.142",
            "port": 8001,
            "table_name": "labels",
            "filename": "discogs_20250401_labels.csv.gz"
        },
        "artists": {
            "url": "http://192.168.1.142",
            "port": 8002,
            "table_name": "artists",
            "filename": "discogs_20250401_artists.csv.gz"
        }
    }
}
```

During deployment, this configuration is:
1. Uploaded to Azure Blob Storage
2. Transformed to use Azure Container App URLs
3. Made available to all services

## Resource Requirements

- Each container (data server and query servers) is allocated 8GB of RAM
- CPU resources are allocated proportionally
- Storage requirements depend on the size of your data files

## Data Storage

- All data files referenced in `config.json` are uploaded to Azure Blob Storage
- The `filename` property in each table configuration points to the file in Azure Blob Storage
- Query servers are configured to access these files directly from storage

## Scaling

The system scales automatically based on the `config.json` file:

- Each table entry in `config.json` results in a dedicated query server
- To add more tables, update `config.json` and redeploy
- Each server maintains its own connection to the data file in Azure Blob Storage

## Troubleshooting

### Common Issues

1. **Deployment Failures**:
   - Check Azure resource name uniqueness
   - Verify Azure subscription has sufficient quota

2. **Container Startup Issues**:
   - Check container logs in Azure Portal
   - Verify environment variables are correctly set

3. **Data Access Issues**:
   - Confirm data files are correctly uploaded to Azure Blob Storage
   - Check storage account access keys and permissions

### Logs and Monitoring

- Container logs are available in Azure Portal
- Application Insights can be added for advanced monitoring

## Security Considerations

- Storage account keys are passed securely as environment variables
- All containers run with least privilege
- Network access can be further restricted using Azure networking features

## Customization

Customize the deployment by modifying:

- `variables.tf`: Change default resource names and locations
- `main.tf`: Adjust container configurations and resource allocations
- `config.json`: Update table definitions and data file references
