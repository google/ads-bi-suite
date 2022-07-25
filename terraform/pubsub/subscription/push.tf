resource "google_pubsub_subscription" "gcf_api_push" {
  ack_deadline_seconds       = 600
  message_retention_duration = "604800s"
  name                       = "gcf-${var.namespace}_api-${var.region}-${var.namespace}-push"

  # push_config {
  #   push_endpoint = "https://f60864dab3576711b4259400a796619f-dot-md3a6ff7882c3e325p-tp.appspot.com/_ah/push-handlers/pubsub/projects/tantan-ua-${var.namespace}/topics/${var.namespace}-push?pubsub_trigger=true"
  # }

  topic = "projects/${var.project_id}/topics/${var.namespace}-push"
}
