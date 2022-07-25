locals {
  need_perms = [
    # Cloud Scheduler Admin
    "cloudscheduler.jobs.create",
    "cloudscheduler.jobs.update",
    # Cloud Datastore User
    "appengine.applications.get",
    "datastore.databases.get",
    "datastore.entities.create",
    "resourcemanager.projects.get",
    # Logs Configuration Writer
    "logging.sinks.create",
    # Pub/Sub Editor
    "pubsub.topics.create",
    "pubsub.subscriptions.create",
    # Storage Admin
    "storage.buckets.list",
    "storage.buckets.create",
    # Service Management Administrator
    "servicemanagement.services.bind",
    # Project IAM Admin
    "resourcemanager.projects.setIamPolicy",
    # Cloud Functions Developer
    "cloudfunctions.functions.create",
    # Service Account User
    "iam.serviceAccounts.actAs",
    # Service Usage Admin
    "serviceusage.services.enable"
  ]
}

data "google_iam_testable_permissions" "perms" {
  full_resource_name = "//cloudresourcemanager.googleapis.com/projects/${var.project_id}"

  lifecycle {
    postcondition {
      condition     = alltrue([for need_p in local.need_perms : contains([for p in self.permissions : p.name], need_p)])
      error_message = "No meet all required permissions. Required permissions: ${join(", \n", local.need_perms)}"
    }
  }
}
