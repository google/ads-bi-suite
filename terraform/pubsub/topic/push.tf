resource "google_pubsub_topic" "push" {
  name    = "${var.namespace}-push"
}
