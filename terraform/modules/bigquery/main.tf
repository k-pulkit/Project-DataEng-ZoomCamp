
terraform {

  required_providers {
    google = {
        source = "hashicorp/google"
        version = "4.51.0"
        configuration_aliases = [google.main,]
    }
  }
}

resource "google_bigquery_dataset" "gcp-dataset" {
    project = var.project
    count = length(var.BQ_DATASET)
    dataset_id = var.BQ_DATASET[count.index]["id"]
    location = var.BQ_DATASET[count.index]["location"]

    labels = {
        "environment" = "development"
    }

}

resource "google_bigquery_table" "gcp_tables" {
  project = var.project
  count = length(var.TABLE_NAME)
  dataset_id = var.TABLE_NAME[count.index]["dataset_id"] 
  table_id = var.TABLE_NAME[count.index]["table_id"] 
  schema = file("${path.root}/${path.module}/schemas/${var.TABLE_NAME[count.index]["schema_id"]}")

  labels = {
        "environment" = "development"
    }

  depends_on = [
    google_bigquery_dataset.gcp-dataset
  ]

  deletion_protection = false
}






