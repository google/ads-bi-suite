resource "google_cloud_scheduler_job" "trdpty_load_reports_job" {
    name        = "${var.namespace}-trdpty_load_reports"
    schedule    = "0 7-23 * * *"
    time_zone   = "${var.timezone}"

    pubsub_target {
        topic_name = "projects/${var.project_id}/topics/${var.namespace}-monitor"
        data       = base64encode("{\"timezone\":\"${var.timezone}\",\n\"partitionDay\": \"$${today}\",\n\"datasetId\": \"${var.ads_report_dataset_id}\"}")
        attributes = {
            taskId = "trdpty_load_reports"
        }
    }

    provisioner "local-exec" {
        command = "gcloud scheduler jobs pause ${self.name}"
    }
}
