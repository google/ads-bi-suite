resource "google_project_service" "cloudscheduler_googleapis_com" {
  service = "cloudscheduler.googleapis.com"
  disable_dependent_services = true
}
