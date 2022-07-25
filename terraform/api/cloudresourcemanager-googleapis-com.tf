resource "google_project_service" "cloudresourcemanager_googleapis_com" {
  service = "cloudresourcemanager.googleapis.com"
  disable_dependent_services = true
}
