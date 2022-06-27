resource "google_pubsub_topic" "monitor" {
  name    = "${var.namespace}-monitor"
}
