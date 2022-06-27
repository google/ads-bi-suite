resource "google_cloud_scheduler_job" "start_hourly_job" {
    name        = "${var.namespace}-${var.namespace}_start_hourly"
    schedule    = "0 7-23 * * *"
    time_zone   = "${var.timezone}"

    pubsub_target {
        topic_name = "projects/${var.project_id}/topics/${var.namespace}-monitor"
        data       = base64encode("{\"timezone\":\"${var.timezone}\",\n\"partitionDay\": \"$${today}\",\n\"datasetId\": \"${var.ads_report_dataset_id}\",\n\"fromDate\": \"$${today_sub_30_hyphenated}\",\n\"developerToken\":\"${var.developer_token}\",\n\"mccCids\": \"${var.mcc_cids}\"}")
        attributes = {
            taskId = "lego_start_hourly"
        }
    }

    provisioner "local-exec" {
        command = "gcloud scheduler jobs pause ${self.name}"
    }
}
