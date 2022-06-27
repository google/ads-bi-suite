resource "google_bigquery_dataset" "adh_audience" {
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

  dataset_id                 = "${var.adh_audience_dataset_id}"
  delete_contents_on_destroy = false
  location                   = "${var.storage_location}"
}
