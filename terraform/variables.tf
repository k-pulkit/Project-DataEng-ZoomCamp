locals {
  data_lake_bucket = "dtc_test_bucket2"
}

variable "credentials" {
  description = "value"
  type = string
}

variable "project" {
  description = "The test project for DE Zoomcamp"
}

variable "region" {
  description = "What region to deploy"
  default = "us-central1"
  type =  string
}

variable "storage_class" {
  description = "value"
  default = "STANDARD"
  type =  string
}

variable "BQ_DATASET" {
  description = "The bigquery datasets to create"
  type =  list(object({
    id = string
    location = string
  }))
}

variable "TABLE_NAME" {
  description = "value"
  type =  list(object({
    dataset_id = string
    table_id = string
    schema_id = string
  }))
}