resource "google_storage_bucket" "gcs_function_bucket" {
  force_destroy               = false
  location                    = "${var.storage_location}"
  name                        = "${var.namespace}-${var.project_id}-function"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
}
