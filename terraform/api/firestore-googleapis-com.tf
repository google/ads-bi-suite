resource "google_project_service" "firestore_googleapis_com" {
  service = "firestore.googleapis.com"
  disable_dependent_services = true
}
