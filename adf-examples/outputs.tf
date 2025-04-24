output "resource_group_name" {
  description = "The name of the resource group"
  value       = azurerm_resource_group.adf_rg.name
}

output "storage_account_name" {
  description = "The name of the storage account"
  value       = azurerm_storage_account.storage.name
}

output "storage_container_name" {
  description = "The name of the storage container"
  value       = azurerm_storage_container.container.name
}

output "key_vault_name" {
  description = "The name of the Key Vault"
  value       = azurerm_key_vault.kv.name
}

output "data_factory_name" {
  description = "The name of the Azure Data Factory"
  value       = azurerm_data_factory.adf.name
}

output "sftp_linked_service_name" {
  description = "The name of the SFTP linked service"
  value       = azurerm_data_factory_linked_service_sftp.sftp_link.name
}

output "pipeline_name" {
  description = "The name of the copy pipeline"
  value       = azurerm_data_factory_pipeline.copy_pipeline.name
}

output "trigger_name" {
  description = "The name of the schedule trigger"
  value       = azurerm_data_factory_trigger_schedule.daily_trigger.name
}
