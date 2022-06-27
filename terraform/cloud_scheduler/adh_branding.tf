resource "google_cloud_scheduler_job" "adh_branding_start_job" {
    name        = "${var.namespace}-adh_branding_start"
    schedule    = "0 11 * * 1"
    time_zone   = "${var.timezone}"

    pubsub_target {
        topic_name = "projects/${var.project_id}/topics/${var.namespace}-monitor"
        data       = base64encode("{\"timezone\":\"${var.timezone}\",\n\"partitionDay\": \"$${today}\",\n\"adhCustomerId\": \"${var.adh_customer_id}\"}")
        attributes = {
            taskId = "adh_branding_start"
        }
    }

    provisioner "local-exec" {
        command = "gcloud scheduler jobs pause ${self.name}"
    }
}
