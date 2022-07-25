resource "google_project_service" "googleads_googleapis_com" {
  service = "googleads.googleapis.com"
  disable_dependent_services = true
}
