resource "google_pubsub_topic" "pb" {
  name    = "${var.namespace}-PB"
}
