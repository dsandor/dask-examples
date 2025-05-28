# Azure Hello World API with PostgreSQL

This project deploys a FastAPI application with PostgreSQL database on Azure using Terraform.

## Prerequisites

1. Azure CLI installed and configured
2. Terraform installed
3. Docker installed
4. Azure Container Registry (ACR) credentials configured

## Deployment Steps

1. Initialize Terraform:
```bash
terraform init
```

2. Build and push the Docker image:
```bash
# Login to Azure Container Registry
az acr login --name helloworldregistry

# Build the Docker image
docker build -t hello-world .

# Tag the image
docker tag hello-world helloworldregistry.azurecr.io/hello-world:latest

# Push the image
docker push helloworldregistry.azurecr.io/hello-world:latest
```

3. Apply Terraform configuration:
```bash
terraform apply
```

## API Endpoints

- `GET /`: Hello World message
- `GET /tables`: List all tables in the database
- `GET /table/{table_name}`: Get data from a specific table (limited to 100 rows)

## Security Notes

- The PostgreSQL password is stored in plain text in the Terraform configuration. In a production environment, use Azure Key Vault to store sensitive information.
- The firewall rule allows access from all Azure services. In production, restrict this to specific IP ranges.
- Consider enabling SSL for the PostgreSQL connection in production.

## Cleanup

To destroy all resources:
```bash
terraform destroy
```

## Solution Summary

This solution includes:

1. **Terraform configuration (`main.tf`)** that sets up:
   - Azure PostgreSQL Server and Database
   - Azure Container Registry
   - Azure Container App Environment and Container App
   - All necessary networking and security configurations

2. **A FastAPI application (`app.py`)** that:
   - Connects to PostgreSQL using SQLAlchemy
   - Provides endpoints to list tables and query data
   - Uses environment variables for database configuration

3. **A `requirements.txt` file** with all necessary Python dependencies

4. **A `Dockerfile`** to containerize the application

5. **A comprehensive `README.md`** with deployment instructions

The solution uses:
- FastAPI for high-performance API endpoints
- SQLAlchemy for efficient database operations
- Python 3.11 slim Docker image for a smaller footprint
- Azure Container Apps for serverless container deployment

The API provides three endpoints:
- `/`: A simple hello world message
- `/tables`: Lists all tables in the database
- `/table/{table_name}`: Returns data from a specific table

**Note:** In a production environment, you should:
- Use Azure Key Vault for sensitive information
- Implement proper network security rules
- Enable SSL for database connections
- Add proper authentication and authorization
- Implement proper error handling and logging
