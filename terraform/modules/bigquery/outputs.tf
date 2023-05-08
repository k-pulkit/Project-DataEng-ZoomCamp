output "project" {
  description = "The GCP project where the resources were created."
  value       = var.project
}

output "region" {
  description = "The GCP region where the resources were created."
  value       = var.region
}

output "credentials" {
  description = "The path to the GCP credentials file used to authenticate with the GCP project."
  value       = var.credentials
}

output "BQ_DATASET_IDS" {
  description = "The dataset ids of the created datasets"
  value = [for bq in var.BQ_DATASET: bq.id]
}

output "BQ_DATASET_LOCATIONS" {
  description = "The dataset location of the created datasets"
  value = [for bq in var.BQ_DATASET: bq.location]
}

output "TABLE_NAME_dataset_ids" {
  description = "The dataset IDs of the BigQuery tables created by the Terraform configuration."
  value       = [for table in var.TABLE_NAME : table.dataset_id]
}

output "TABLE_NAME_table_ids" {
  description = "The table IDs of the BigQuery tables created by the Terraform configuration."
  value       = [for table in var.TABLE_NAME : table.table_id]
}

output "TABLE_NAME_schema_ids" {
  description = "The schema IDs of the BigQuery tables created by the Terraform configuration."
  value       = [for table in var.TABLE_NAME : table.schema_id]
}