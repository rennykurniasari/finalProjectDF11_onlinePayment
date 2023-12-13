variable "project" {
  description = "Project name"
  type        = string
}

variable "credentials_project" {
  description = "JSON key file path service account for Project"
  type        = string
}

variable "name_dataset" {
  description = "Dataset name"
  type        = string
}

variable "bucket_gcs" {
  description = "Bucket GCS name"
  type        = string
}

variable "folder_gcs_email" {
  description = "GCS folder for email confirmation data"
  type        = string
}

variable "folder_gcs_fraud" {
  description = "GCS folder for fraud confirmation data"
  type        = string
}

variable "folder_gcs_transaction" {
  description = "GCS folder for transaction data"
  type        = string
}

variable "network" {
  description = "Network name"
  type        = string
}

variable "vm_type" {
  description = "VM type"
  type        = string
  default = "e2-standard-4"
}

variable "public_key_vm" {
  description = "JSON public key file path for SSH VM connection"
  type        = string
}

variable "private_key_vm" {
  description = "JSON public key file path for SSH VM connection"
  type        = string
}

variable "user_gcp" {
  description = "Name user GCP"
  type        = string
}