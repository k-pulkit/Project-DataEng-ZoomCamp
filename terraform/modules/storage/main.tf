
terraform {

  required_providers {
    google = {
        source = "hashicorp/google"
        version = "4.51.0"
        configuration_aliases = [google.main,]
    }
  }
}


resource "google_storage_bucket" "my_bucket" {
 name          = "${var.data_lake_bucket}_${var.project}"
 location      = var.region
 storage_class = var.storage_class

 uniform_bucket_level_access = true

 versioning {
   enabled = true
 }

 force_destroy = true
}