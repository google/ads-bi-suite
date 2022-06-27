resource "google_cloudfunctions_function" "requester" {
    name                  = "${var.namespace}_api"
    runtime               = "nodejs14"
    region                = "${var.region}"

    source_archive_bucket = "${var.namespace}-${var.project_id}-function"
    source_archive_object = google_storage_bucket_object.cloud_function_zip.name

    entry_point           = "requestApi"

    available_memory_mb   = 2048
    timeout               = 540

    event_trigger {
        event_type = "google.pubsub.topic.publish"
        resource   = "projects/${var.project_id}/topics/${var.namespace}-push"
    }

    environment_variables = {
        GCP_PROJECT="${var.project_id}"
        PROJECT_NAMESPACE="${var.namespace}"
        DEBUG="false"
        IN_GCP="true"
        OAUTH2_TOKEN_JSON="./keys/oauth2.token.json"
    }

    depends_on            = [
        google_storage_bucket_object.cloud_function_zip
    ]
}
