terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Create resource group
resource "azurerm_resource_group" "bigdata_rg" {
  name     = var.resource_group_name
  location = var.location
}

# Create storage account for data files
resource "azurerm_storage_account" "data_storage" {
  name                     = var.storage_account_name
  resource_group_name      = azurerm_resource_group.bigdata_rg.name
  location                 = azurerm_resource_group.bigdata_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# Create storage container for data files
resource "azurerm_storage_container" "data_container" {
  name                  = var.storage_container_name
  storage_account_name  = azurerm_storage_account.data_storage.name
  container_access_type = "private"
}

# Upload config.json to blob storage
resource "azurerm_storage_blob" "config_blob" {
  name                   = "config.json"
  storage_account_name   = azurerm_storage_account.data_storage.name
  storage_container_name = azurerm_storage_container.data_container.name
  type                   = "Block"
  source                 = "${path.module}/../config.json"
}

# Upload data files to blob storage
resource "azurerm_storage_blob" "data_files" {
  for_each = {
    for table_name, table_config in jsondecode(file("${path.module}/../config.json")).tables :
    table_name => table_config.filename
  }

  name                   = each.value
  storage_account_name   = azurerm_storage_account.data_storage.name
  storage_container_name = azurerm_storage_container.data_container.name
  type                   = "Block"
  source                 = "${path.module}/../data/${each.value}"
  content_type           = "application/gzip"
}

# Create Container Registry for Docker images
resource "azurerm_container_registry" "acr" {
  name                = var.container_registry_name
  resource_group_name = azurerm_resource_group.bigdata_rg.name
  location            = azurerm_resource_group.bigdata_rg.location
  sku                 = "Basic"
  admin_enabled       = true
}

# Create Container App Environment
resource "azurerm_container_app_environment" "container_env" {
  name                       = var.container_app_environment_name
  location                   = azurerm_resource_group.bigdata_rg.location
  resource_group_name        = azurerm_resource_group.bigdata_rg.name
}

# Deploy Data Server Container App
resource "azurerm_container_app" "data_server" {
  name                         = "data-server"
  container_app_environment_id = azurerm_container_app_environment.container_env.id
  resource_group_name          = azurerm_resource_group.bigdata_rg.name
  revision_mode                = "Single"

  template {
    container {
      name   = "data-server"
      image  = "${azurerm_container_registry.acr.login_server}/data-server:latest"
      cpu    = 2.0
      memory = "8Gi"
      
      env {
        name  = "AWS_ACCESS_KEY_ID"
        value = azurerm_storage_account.data_storage.primary_access_key
      }
      
      env {
        name  = "AWS_SECRET_ACCESS_KEY"
        value = azurerm_storage_account.data_storage.secondary_access_key
      }
      
      env {
        name  = "AWS_DEFAULT_REGION"
        value = "eastus"
      }
      
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.data_storage.name
      }
      
      env {
        name  = "AZURE_STORAGE_CONTAINER"
        value = azurerm_storage_container.data_container.name
      }
    }
  }

  ingress {
    external_enabled = true
    target_port      = 8000
    transport        = "http"
  }
}

# Deploy Distributed Query Servers (one for each table)
resource "azurerm_container_app" "query_servers" {
  for_each = {
    for table_name, table_config in jsondecode(file("${path.module}/../config.json")).tables :
    table_name => {
      port      = table_config.port
      filename  = table_config.filename
      table_name = table_config.table_name
    }
  }

  name                         = "query-server-${each.key}"
  container_app_environment_id = azurerm_container_app_environment.container_env.id
  resource_group_name          = azurerm_resource_group.bigdata_rg.name
  revision_mode                = "Single"

  template {
    container {
      name   = "query-server-${each.key}"
      image  = "${azurerm_container_registry.acr.login_server}/query-server:latest"
      cpu    = 2.0
      memory = "8Gi"
      
      command = [
        "python", 
        "/app/query_s3.py", 
        "--url", "s3://${azurerm_storage_account.data_storage.name}/${azurerm_storage_container.data_container.name}/${each.value.filename}",
        "--table-name", each.value.table_name
      ]
      
      env {
        name  = "AWS_ACCESS_KEY_ID"
        value = azurerm_storage_account.data_storage.primary_access_key
      }
      
      env {
        name  = "AWS_SECRET_ACCESS_KEY"
        value = azurerm_storage_account.data_storage.secondary_access_key
      }
      
      env {
        name  = "AWS_DEFAULT_REGION"
        value = "eastus"
      }
      
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.data_storage.name
      }
      
      env {
        name  = "AZURE_STORAGE_CONTAINER"
        value = azurerm_storage_container.data_container.name
      }
    }
  }

  ingress {
    external_enabled = true
    target_port      = each.value.port
    transport        = "http"
  }
}

# Deploy Distributed Query Coordinator
resource "azurerm_container_app" "query_coordinator" {
  name                         = "query-coordinator"
  container_app_environment_id = azurerm_container_app_environment.container_env.id
  resource_group_name          = azurerm_resource_group.bigdata_rg.name
  revision_mode                = "Single"

  template {
    container {
      name   = "query-coordinator"
      image  = "${azurerm_container_registry.acr.login_server}/distributed-query-server:latest"
      cpu    = 2.0
      memory = "8Gi"
      
      env {
        name  = "CONFIG_URL"
        value = "https://${azurerm_storage_account.data_storage.name}.blob.core.windows.net/${azurerm_storage_container.data_container.name}/config.json"
      }
      
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.data_storage.name
      }
      
      env {
        name  = "AZURE_STORAGE_CONTAINER"
        value = azurerm_storage_container.data_container.name
      }
      
      env {
        name  = "AZURE_STORAGE_KEY"
        value = azurerm_storage_account.data_storage.primary_access_key
      }
    }
  }

  ingress {
    external_enabled = true
    target_port      = 8000
    transport        = "http"
  }
}
