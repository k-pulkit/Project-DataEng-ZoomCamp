locals {
  data_lake_bucket = "dtc_test_bucket"
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

variable "bucket_name" {
  description = "value"
  default = ""
  type =  string
}

variable "storage_class" {
  description = "value"
  default = "STANDARD"
  type =  string
}

variable "BQ_DATASET" {
  description = "value"
  default = "trips_data_all"
  type =  string
}

variable "TABLE_NAME" {
  description = "value"
  default = "test_1"
  type =  string
}