resource "google_pubsub_topic" "trigger" {
  name    = "${var.namespace}-trigger"
}
