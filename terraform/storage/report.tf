resource "google_storage_bucket" "gcs_report" {
  force_destroy               = false

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age        = 3
      with_state = "ANY"
    }
  }

  location                    = "${var.storage_location}"
  name                        = "${var.namespace}-${var.project_id}"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
}
