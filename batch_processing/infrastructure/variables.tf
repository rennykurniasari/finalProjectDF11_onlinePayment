variable "project" {
  description = "Project name"
  type        = string
}

variable "credentials_project" {
  description = "JSON key file path service account for Project"
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
