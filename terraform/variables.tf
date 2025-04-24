variable "resource_group_name" {
  description = "Name of the Azure resource group"
  type        = string
  default     = "bigdata-rg"
}

variable "location" {
  description = "Azure region to deploy resources"
  type        = string
  default     = "eastus"
}

variable "storage_account_name" {
  description = "Name of the Azure Storage account"
  type        = string
  default     = "bigdatastorage"
}

variable "storage_container_name" {
  description = "Name of the Azure Storage container"
  type        = string
  default     = "data"
}

variable "container_registry_name" {
  description = "Name of the Azure Container Registry"
  type        = string
  default     = "bigdataregistry"
}

variable "container_app_environment_name" {
  description = "Name of the Azure Container App Environment"
  type        = string
  default     = "bigdata-env"
}
