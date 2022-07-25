resource "google_logging_project_sink" "lego_monitor" {
  destination            = "pubsub.googleapis.com/projects/${var.project_id}/topics/${var.namespace}-monitor"
  filter                 = "resource.type=\"bigquery_resource\" AND protoPayload.methodName=\"jobservice.jobcompleted\""
  name                   = "${var.namespace}-monitor"
  unique_writer_identity = true
}

resource "google_project_iam_binding" "project" {
  project = "${var.project_id}"
  role    = "roles/pubsub.publisher"

  members = [
    "${google_logging_project_sink.lego_monitor.writer_identity}",
  ]

  depends_on = [
    google_logging_project_sink.lego_monitor
  ]
}