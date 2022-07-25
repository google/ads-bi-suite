resource "google_project_service" "iam_googleapis_com" {
  service = "iam.googleapis.com"
  disable_dependent_services = true
}
