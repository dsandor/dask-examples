variable "location" {
  description = "Azure region to deploy resources"
  type        = string
  default     = "eastus"
}

variable "sftp_host" {
  description = "SFTP server hostname"
  type        = string
  default     = "fake-sftp.example.com"
}

variable "sftp_port" {
  description = "SFTP server port"
  type        = number
  default     = 22
}

variable "sftp_path" {
  description = "SFTP server path to monitor for files"
  type        = string
  default     = "/upload/"
}

variable "sftp_file_pattern" {
  description = "File pattern to copy from SFTP server"
  type        = string
  default     = "*.csv"
}

variable "storage_container_name" {
  description = "Name of the storage container"
  type        = string
  default     = "adf-example-bucket"
}

variable "storage_destination_path" {
  description = "Path within the storage container to store files"
  type        = string
  default     = "sftp-data/"
}
