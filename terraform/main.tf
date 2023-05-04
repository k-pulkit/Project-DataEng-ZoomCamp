terraform {
  required_version = ">=1.0.0"
  backend "local" {
    
  }

  required_providers {
    google = {
        source = "hashicorp/google"
        version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project = var.project
  region = var.region
}

resource "google_storage_bucket" "my_test_bucket" {
 name          = "${local.data_lake_bucket}_${var.project}"
 location      = var.region
 storage_class = var.storage_class

 uniform_bucket_level_access = true

 versioning {
   enabled = true
 }

 force_destroy = true
}

resource "google_bigquery_dataset" "test_dataset" {
    project = var.project
    count = length(var.BQ_DATASET)
    dataset_id = var.BQ_DATASET[count.index]["id"]
    location = var.BQ_DATASET[count.index]["location"]

    labels = {
        "environment" = "development"
    }

}

resource "google_bigquery_table" "test_tables" {
  project = var.project
  count = length(var.TABLE_NAME)
  dataset_id = var.TABLE_NAME[count.index]["dataset_id"] 
  table_id = var.TABLE_NAME[count.index]["table_id"] 
  schema = file("${path.root}/schemas/${var.TABLE_NAME[count.index]["schema_id"]}")

  labels = {
        "environment" = "development"
    }

  depends_on = [
    google_bigquery_dataset.test_dataset
  ]

  deletion_protection = false
}






