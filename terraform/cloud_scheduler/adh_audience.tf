resource "google_cloud_scheduler_job" "adh_audience_start_job" {
    name        = "${var.namespace}-adh_audience_start"
    schedule    = "0 15 * * 1"
    time_zone   = "${var.timezone}"

    pubsub_target {
        topic_name = "projects/${var.project_id}/topics/${var.namespace}-monitor"
        data       = base64encode("{\"timezone\":\"${var.timezone}\",\n\"partitionDay\": \"$${today}\",\n\"legoDatasetId\": \"${var.ads_report_dataset_id}\",\n\"adhCustomerId\": \"${var.adh_customer_id}\"}")
        attributes = {
            taskId = "adh_audience_start"
        }
    }

    provisioner "local-exec" {
        command = "gcloud scheduler jobs pause ${self.name}"
    }
}