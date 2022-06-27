resource "google_pubsub_subscription" "gcf_tran_trigger" {
  ack_deadline_seconds       = 600
  message_retention_duration = "604800s"
  name                       = "gcf-${var.namespace}_tran-${var.region}-${var.namespace}-trigger"

  # push_config {
  #   push_endpoint = "https://a52ee35e6739db1a6be0132c20031470-dot-md3a6ff7882c3e325p-tp.appspot.com/_ah/push-handlers/pubsub/projects/tantan-ua-lego/topics/lego-trigger?pubsub_trigger=true"
  # }

  topic = "projects/${var.project_id}/topics/${var.namespace}-trigger"
}
