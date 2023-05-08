terraform {
  required_version = ">=1.0.0"
  backend "local" {
    
  }

  required_providers {
    google = {
        source = "hashicorp/google"
        version = "4.51.0"
        configuration_aliases = [google.main,]
    }
  }
}

provider "google" {
  alias = "main"
}

module "storage" {
  source = "./modules/storage"
  providers = {
    google.main = google.main
  }
  project = var.project
  region = var.region
  storage_class = var.storage_class
  data_lake_bucket = local.data_lake_bucket
  credentials = var.credentials
}

module "bigquery" {
  source = "./modules/bigquery"
  providers = {
    google.main = google.main
  }
  project = var.project
  region = var.region
  BQ_DATASET = var.BQ_DATASET
  TABLE_NAME = var.TABLE_NAME
  credentials = var.credentials
}
