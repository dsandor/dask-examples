#!/bin/bash
set -e

# Get variables from terraform output or use defaults
ACR_NAME=${1:-bigdataregistry}
RESOURCE_GROUP=${2:-bigdata-rg}
LOCATION=${3:-eastus}

echo "Building and pushing Docker images to Azure Container Registry: $ACR_NAME"

# Create resource group if it doesn't exist
echo "Creating resource group $RESOURCE_GROUP if it doesn't exist..."
az group create --name $RESOURCE_GROUP --location $LOCATION

# Create ACR if it doesn't exist
echo "Creating Azure Container Registry $ACR_NAME if it doesn't exist..."
az acr create --resource-group $RESOURCE_GROUP --name $ACR_NAME --sku Basic --admin-enabled true

# Get ACR login credentials
echo "Getting ACR login credentials..."
ACR_USERNAME=$(az acr credential show --name $ACR_NAME --query "username" -o tsv)
ACR_PASSWORD=$(az acr credential show --name $ACR_NAME --query "passwords[0].value" -o tsv)

# Login to ACR
echo "Logging in to ACR..."
az acr login --name $ACR_NAME

# Build and push data server image
echo "Building and pushing data server image..."
cd ..
docker build -t ${ACR_NAME}.azurecr.io/data-server:latest -f Dockerfile .
docker push ${ACR_NAME}.azurecr.io/data-server:latest

# Build and push distributed query server image
echo "Building and pushing distributed query server image..."
docker build -t ${ACR_NAME}.azurecr.io/distributed-query-server:latest -f Dockerfile.distributed .
docker push ${ACR_NAME}.azurecr.io/distributed-query-server:latest

# Also tag and push the query server image (which is the same as data-server but with different entrypoint)
echo "Tagging and pushing query server image..."
docker tag ${ACR_NAME}.azurecr.io/data-server:latest ${ACR_NAME}.azurecr.io/query-server:latest
docker push ${ACR_NAME}.azurecr.io/query-server:latest

echo "All images built and pushed successfully!"
