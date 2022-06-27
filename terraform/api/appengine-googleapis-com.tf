resource "google_project_service" "appengine_googleapis_com" {
  service = "appengine.googleapis.com"
  disable_dependent_services = true
}
