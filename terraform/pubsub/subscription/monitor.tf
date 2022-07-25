resource "google_pubsub_subscription" "gcf_main_monitor" {
  ack_deadline_seconds       = 600
  message_retention_duration = "604800s"
  name                       = "gcf-${var.namespace}_main-${var.region}-${var.namespace}-monitor"

  # push_config {
  #   push_endpoint = "https://352aa2cd8d9853b893eba1586d10d070-dot-md3a6ff7882c3e325p-tp.appspot.com/_ah/push-handlers/pubsub/projects/tantan-ua-lego/topics/lego-monitor?pubsub_trigger=true"
  # }

  topic = "projects/${var.project_id}/topics/${var.namespace}-monitor"
}
