variable "gcp_project_id" {
  description = "ID Project GCP tempat sumber daya akan dibuat."
  type        = string
}

variable "gcp_region" {
  description = "Region GCP untuk semua sumber daya (misalnya, asia-southeast2)."
  type        = string
  default     = "asia-southeast2"
}
