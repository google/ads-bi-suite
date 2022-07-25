resource "google_cloud_scheduler_job" "intrinsic_job" {
    name        = "${var.namespace}-intrinsic-cronjob"
    schedule    = "*/5 * * * *"
    time_zone   = "${var.timezone}"

    pubsub_target {
        topic_name = "projects/${var.project_id}/topics/${var.namespace}-monitor"
        data       = base64encode("{\"intrinsic\":\"status_check\"}")
        attributes = {
            taskId = "system"
        }
    }
}
