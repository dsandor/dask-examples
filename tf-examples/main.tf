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

resource "azurerm_resource_group" "rg" {
  name     = "hello-world-rg"
  location = "East US"
}

# PostgreSQL Server
resource "azurerm_postgresql_server" "pg_server" {
  name                = "hello-world-pg-server"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  sku_name = "B_Gen5_1"

  storage_mb                   = 51200
  backup_retention_days        = 7
  geo_redundant_backup_enabled = false
  auto_grow_enabled           = true

  administrator_login          = "psqladmin"
  administrator_login_password = "P@ssw0rd1234!"  # In production, use Azure Key Vault
  version                     = "11"
  ssl_enforcement_enabled     = true
}

# PostgreSQL Database
resource "azurerm_postgresql_database" "pg_db" {
  name                = "helloworlddb"
  resource_group_name = azurerm_resource_group.rg.name
  server_name         = azurerm_postgresql_server.pg_server.name
  charset             = "UTF8"
  collation          = "English_United States.1252"
}

# PostgreSQL Firewall Rule
resource "azurerm_postgresql_firewall_rule" "pg_fw" {
  name                = "allow-azure-services"
  resource_group_name = azurerm_resource_group.rg.name
  server_name         = azurerm_postgresql_server.pg_server.name
  start_ip_address    = "0.0.0.0"
  end_ip_address      = "0.0.0.0"
}

# Container Registry
resource "azurerm_container_registry" "acr" {
  name                = "helloworldregistry"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "Basic"
  admin_enabled       = true
}

# Container App Environment
resource "azurerm_container_app_environment" "env" {
  name                       = "hello-world-env"
  location                   = azurerm_resource_group.rg.location
  resource_group_name        = azurerm_resource_group.rg.name
}

# Container App
resource "azurerm_container_app" "app" {
  name                         = "hello-world-app"
  container_app_environment_id = azurerm_container_app_environment.env.id
  resource_group_name          = azurerm_resource_group.rg.name
  revision_mode                = "Single"

  template {
    container {
      name   = "hello-world"
      image  = "${azurerm_container_registry.acr.login_server}/hello-world:latest"
      cpu    = 0.5
      memory = "1Gi"

      env {
        name  = "DB_HOST"
        value = azurerm_postgresql_server.pg_server.fqdn
      }
      env {
        name  = "DB_NAME"
        value = azurerm_postgresql_database.pg_db.name
      }
      env {
        name  = "DB_USER"
        value = azurerm_postgresql_server.pg_server.administrator_login
      }
      env {
        name  = "DB_PASSWORD"
        value = azurerm_postgresql_server.pg_server.administrator_login_password
      }
    }
  }
} 