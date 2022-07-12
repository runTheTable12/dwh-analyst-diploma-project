variable "yandex_cloud_token" {
  description = "yandex cloud token"
  type        = string
  sensitive   = true
}

variable "yandex_cloud_id" {
  description = "yandex cloud id"
  type        = string
  sensitive   = true
}

variable "yandex_cloud_folder_id" {
  description = "yandex cloud folder id"
  type        = string
  sensitive   = true
}

variable "db_name" {
  description = "name of a project database"
  type        = string
  sensitive   = true
}

variable "db_username" {
  description = "username for a project database"
  type        = string
  sensitive   = true
}

variable "db_password" {
  description = "username password for a project database"
  type        = string
  sensitive   = true
}