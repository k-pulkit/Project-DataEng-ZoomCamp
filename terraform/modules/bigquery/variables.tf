variable "project" {
  type = string
}

variable "region" {
  type = string
}

variable "credentials" {
  type = string
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