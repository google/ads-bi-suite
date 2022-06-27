resource "google_cloudfunctions_function" "initiator" {
    name                  = "${var.namespace}_init"
    runtime               = "nodejs14"
    region                = "${var.region}"

    source_archive_bucket = "${var.namespace}-${var.project_id}-function"
    source_archive_object = google_storage_bucket_object.cloud_function_zip.name

    entry_point           = "initiate"

    available_memory_mb   = 2048
    timeout               = 540

    event_trigger {
        event_type = "google.storage.object.finalize"
        resource   = "${var.namespace}-${var.project_id}"
    }

    environment_variables = {
        TENTACLES_OUTBOUND="${var.tentacles_outbound}"
        GCP_PROJECT="${var.project_id}"
        PROJECT_NAMESPACE="${var.namespace}"
        DEBUG="false"
        IN_GCP="true"
    }

    depends_on            = [
        google_storage_bucket_object.cloud_function_zip
    ]
}
