output "storage_account_name" {
  description = "The name of the storage account"
  value       = azurerm_storage_account.data_storage.name
}

output "storage_account_primary_access_key" {
  description = "The primary access key for the storage account"
  value       = azurerm_storage_account.data_storage.primary_access_key
  sensitive   = true
}

output "storage_container_name" {
  description = "The name of the storage container"
  value       = azurerm_storage_container.data_container.name
}

output "container_registry_login_server" {
  description = "The login server URL for the container registry"
  value       = azurerm_container_registry.acr.login_server
}

output "container_registry_admin_username" {
  description = "The admin username for the container registry"
  value       = azurerm_container_registry.acr.admin_username
}

output "container_registry_admin_password" {
  description = "The admin password for the container registry"
  value       = azurerm_container_registry.acr.admin_password
  sensitive   = true
}

output "data_server_url" {
  description = "The URL of the data server"
  value       = azurerm_container_app.data_server.latest_revision_fqdn
}

output "query_servers" {
  description = "The URLs of the query servers"
  value = {
    for table_name, app in azurerm_container_app.query_servers :
    table_name => app.latest_revision_fqdn
  }
}

output "query_coordinator_url" {
  description = "The URL of the query coordinator"
  value       = azurerm_container_app.query_coordinator.latest_revision_fqdn
}
