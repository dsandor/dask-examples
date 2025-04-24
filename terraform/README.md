# Azure Deployment for Big Data Query System

This Terraform configuration deploys a distributed data query system to Azure Container Apps with Azure Blob Storage for data files.

## Architecture

The deployment consists of:

1. **Azure Storage Account** - Stores all data files and the configuration file
2. **Azure Container Registry** - Stores Docker images
3. **Azure Container Apps Environment** - Hosts all container instances
4. **Data Server Container** - Main data server with 8GB RAM
5. **Query Server Containers** - One container per table defined in config.json, each with 8GB RAM
6. **Query Coordinator Container** - Coordinates distributed queries across query servers

## Prerequisites

- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
- [Terraform](https://www.terraform.io/downloads.html)
- [Docker](https://docs.docker.com/get-docker/)
- Azure subscription

## Setup

1. Log in to Azure CLI:

```bash
az login
```

2. Make the build script executable:

```bash
chmod +x build_and_push.sh
```

3. Build and push Docker images to Azure Container Registry:

```bash
./build_and_push.sh
```

4. Initialize Terraform:

```bash
terraform init
```

5. Create a `terraform.tfvars` file (optional) to customize variables:

```
resource_group_name = "your-resource-group"
location = "eastus"
storage_account_name = "yourstorageaccount"
container_registry_name = "yourregistry"
```

6. Create a plan:

```bash
terraform plan -out=tfplan
```

7. Apply the Terraform configuration:

```bash
terraform apply tfplan
```

## Configuration

The deployment uses the `config.json` file in the parent directory to determine:

1. What data files to upload to Azure Blob Storage
2. How many query server instances to create (one per table)
3. The configuration for each query server

For each table defined in the `config.json`, the system will:

1. Upload the corresponding data file to Azure Blob Storage
2. Create a dedicated query server container with 8GB RAM
3. Configure the query server to point to the correct data file

## Accessing the Services

After deployment, Terraform will output:

- URLs for each query server
- URL for the query coordinator
- Storage account information
- Container registry information

## Customization

You can customize the deployment by modifying:

- `variables.tf` - Default values for resource names and locations
- `terraform.tfvars` - Override default values (create this file if needed)
- `main.tf` - Main infrastructure configuration

## Important Notes

1. Each container is configured with 8GB of RAM as specified
2. The system automatically creates one query server per table in config.json
3. All data files referenced in config.json should be present in the `../data/` directory
4. Azure Storage credentials are automatically passed to containers
5. The config.json is uploaded to Azure Blob Storage and accessible to all containers
