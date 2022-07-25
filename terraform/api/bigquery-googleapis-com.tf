resource "google_project_service" "bigquery_googleapis_com" {
  service = "bigquery.googleapis.com"
  disable_dependent_services = true
}
