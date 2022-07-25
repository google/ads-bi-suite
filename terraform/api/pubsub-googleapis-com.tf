resource "google_project_service" "pubsub_googleapis_com" {
  service = "pubsub.googleapis.com"
  disable_dependent_services = true
}
