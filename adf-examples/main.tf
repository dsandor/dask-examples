terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = true
    }
  }
}

# Create a resource group
resource "azurerm_resource_group" "adf_rg" {
  name     = "adf-example-rg"
  location = var.location
}

# Create a random string for unique resource names
resource "random_string" "unique" {
  length  = 8
  special = false
  upper   = false
}

# Create a storage account
resource "azurerm_storage_account" "storage" {
  name                     = "adfexample${random_string.unique.result}"
  resource_group_name      = azurerm_resource_group.adf_rg.name
  location                 = azurerm_resource_group.adf_rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}

# Create a storage container
resource "azurerm_storage_container" "container" {
  name                 = var.storage_container_name
  storage_account_name = azurerm_storage_account.storage.name
  container_access_type = "private"
}

# Create a Key Vault
resource "azurerm_key_vault" "kv" {
  name                        = "adf-kv-${random_string.unique.result}"
  location                    = azurerm_resource_group.adf_rg.location
  resource_group_name         = azurerm_resource_group.adf_rg.name
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false

  sku_name = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id

    key_permissions = [
      "Get", "List", "Create", "Delete", "Update",
    ]

    secret_permissions = [
      "Get", "List", "Set", "Delete",
    ]

    certificate_permissions = [
      "Get", "List", "Create", "Delete",
    ]
  }
}

# Get current client configuration
data "azurerm_client_config" "current" {}

# Create Key Vault secrets for SFTP credentials
resource "azurerm_key_vault_secret" "sftp_username" {
  name         = "sftp-username"
  value        = "fake-sftp-user"
  key_vault_id = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "sftp_password" {
  name         = "sftp-password"
  value        = "ThisIsAFakePassword123!"
  key_vault_id = azurerm_key_vault.kv.id
}

# Create Azure Data Factory
resource "azurerm_data_factory" "adf" {
  name                = "adf-example-${random_string.unique.result}"
  location            = azurerm_resource_group.adf_rg.location
  resource_group_name = azurerm_resource_group.adf_rg.name
}

# Create a linked service for Key Vault
resource "azurerm_data_factory_linked_service_key_vault" "kv_link" {
  name            = "KeyVaultLinkedService"
  data_factory_id = azurerm_data_factory.adf.id
  key_vault_id    = azurerm_key_vault.kv.id
}

# Create a linked service for SFTP
resource "azurerm_data_factory_linked_service_sftp" "sftp_link" {
  name                = "SftpLinkedService"
  data_factory_id     = azurerm_data_factory.adf.id
  authentication_type = "Basic"
  host                = var.sftp_host
  port                = var.sftp_port
  username            = azurerm_key_vault_secret.sftp_username.value
  password            = azurerm_key_vault_secret.sftp_password.value
}

# Create a linked service for Azure Blob Storage
resource "azurerm_data_factory_linked_service_azure_blob_storage" "blob_link" {
  name            = "BlobStorageLinkedService"
  data_factory_id = azurerm_data_factory.adf.id
  connection_string = azurerm_storage_account.storage.primary_connection_string
}

# Create a custom dataset for SFTP source
resource "azurerm_data_factory_custom_dataset" "sftp_dataset" {
  name            = "SftpSourceDataset"
  data_factory_id = azurerm_data_factory.adf.id
  type            = "Binary"

  linked_service {
    name = azurerm_data_factory_linked_service_sftp.sftp_link.name
  }

  type_properties_json = <<JSON
{
  "location": {
    "type": "SftpLocation",
    "folderPath": "${var.sftp_path}",
    "fileName": "${var.sftp_file_pattern}"
  }
}
JSON
}

# Create a custom dataset for Blob Storage destination
resource "azurerm_data_factory_custom_dataset" "blob_dataset" {
  name            = "BlobDestinationDataset"
  data_factory_id = azurerm_data_factory.adf.id
  type            = "Binary"

  linked_service {
    name = azurerm_data_factory_linked_service_azure_blob_storage.blob_link.name
  }

  type_properties_json = <<JSON
{
  "location": {
    "type": "AzureBlobStorageLocation",
    "container": "${var.storage_container_name}",
    "folderPath": "${var.storage_destination_path}",
    "fileName": "{filename}"
  }
}
JSON
}

# Create a pipeline to copy data from SFTP to Blob Storage
resource "azurerm_data_factory_pipeline" "copy_pipeline" {
  name            = "CopySftpToBlob"
  data_factory_id = azurerm_data_factory.adf.id
  
  activities_json = <<JSON
[
  {
    "name": "CopyFromSftpToBlob",
    "type": "Copy",
    "dependsOn": [],
    "policy": {
      "timeout": "7.00:00:00",
      "retry": 0,
      "retryIntervalInSeconds": 30,
      "secureOutput": false,
      "secureInput": false
    },
    "userProperties": [],
    "typeProperties": {
      "source": {
        "type": "BinarySource",
        "storeSettings": {
          "type": "SftpReadSettings",
          "recursive": true,
          "wildcardFileName": "${var.sftp_file_pattern}"
        },
        "formatSettings": {
          "type": "BinaryReadSettings"
        }
      },
      "sink": {
        "type": "BinarySink",
        "storeSettings": {
          "type": "AzureBlobStorageWriteSettings"
        }
      },
      "enableStaging": false
    },
    "inputs": [
      {
        "referenceName": "${azurerm_data_factory_custom_dataset.sftp_dataset.name}",
        "type": "DatasetReference"
      }
    ],
    "outputs": [
      {
        "referenceName": "${azurerm_data_factory_custom_dataset.blob_dataset.name}",
        "type": "DatasetReference"
      }
    ]
  }
]
JSON
}

# Create a trigger to run the pipeline daily
resource "azurerm_data_factory_trigger_schedule" "daily_trigger" {
  name            = "DailyTrigger"
  data_factory_id = azurerm_data_factory.adf.id
  pipeline_name   = azurerm_data_factory_pipeline.copy_pipeline.name
  
  interval  = 1
  frequency = "Day"
  start_time = "${formatdate("YYYY-MM-DD", timestamp())}T00:00:00Z"
}
