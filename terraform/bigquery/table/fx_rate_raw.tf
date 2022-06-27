resource "google_bigquery_table" "fx_rate_raw" {
  dataset_id = "${var.config_dataset_id}"

  external_data_configuration {
    autodetect = true

    google_sheets_options {
      range             = "fx rate"
      skip_leading_rows = 1
    }

    source_format = "GOOGLE_SHEETS"
    source_uris   = ["https://docs.google.com/spreadsheets/d/${var.fx_rate_spreadsheet_id}/edit"]
  }

  schema   = "[{\"mode\":\"NULLABLE\",\"name\":\"fromcur\",\"type\":\"STRING\"},{\"mode\":\"NULLABLE\",\"name\":\"tocur\",\"type\":\"STRING\"},{\"mode\":\"NULLABLE\",\"name\":\"rate\",\"type\":\"FLOAT\"}]"
  table_id = "fx_rate_raw"
}
