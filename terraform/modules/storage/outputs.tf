

output "bucket_name" {
  description = "The name of the GCS bucket created by the 'module.storage' module."
  value       = google_storage_bucket.my_bucket.name
}

output "bucket_url" {
  description = "The URL of the GCS bucket created by the 'module.storage' module."
  value       = google_storage_bucket.my_bucket.self_link
}

output "bucket_location" {
  description = "The location of the GCS bucket created by the 'module.storage' module."
  value       = google_storage_bucket.my_bucket.location
}

output "bucket_storage_class" {
  description = "The storage class of the GCS bucket created by the 'module.storage' module."
  value       = google_storage_bucket.my_bucket.storage_class
}

output "bucket_versioning_enabled" {
  description = "Whether versioning is enabled for the GCS bucket created by the 'module.storage' module."
  value       = google_storage_bucket.my_bucket.versioning[0].enabled
}
