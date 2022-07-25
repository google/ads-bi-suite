locals {
    sql_files = sort(fileset("../sql", "*.sql"))
}

resource "google_storage_bucket" "gcs_config" {
  force_destroy               = false
  location                    = "${var.storage_location}"
  name                        = "${var.namespace}-${var.project_id}-config"
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
}

resource "google_storage_bucket_object" "sql_to_gcs_config" {
  count = "${length(local.sql_files)}"

  name = "sql/${basename(element(local.sql_files, count.index))}"
  source = "../sql/${element(local.sql_files, count.index)}"
  bucket = google_storage_bucket.gcs_config.name

  depends_on = [
    google_storage_bucket.gcs_config
  ]
}