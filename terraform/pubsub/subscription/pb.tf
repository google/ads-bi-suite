resource "google_pubsub_subscription" "pb_holder" {
  ack_deadline_seconds       = 300
  message_retention_duration = "604800s"
  name                       = "${var.namespace}-PB-holder"
  topic                      = "projects/${var.project_id}/topics/${var.namespace}-PB"
}
