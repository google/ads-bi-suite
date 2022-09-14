# Google provide reference: https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = "us-central1-c"
}

module "billing" {
  source = "./billing"
  project_id = var.project_id
}

module "permission" {
  source = "./permission"
  project_id = var.project_id
  depends_on = [module.billing]
}

module "api" {
  source = "./api"
  lego_functions = var.lego_functions

  depends_on = [module.permission]
}

module "storage" {
  source = "./storage"
  storage_location = var.storage_location
  namespace = var.namespace
  project_id = var.project_id

  depends_on = [module.api]
}

module "bigquery_dataset" {
  source = "./bigquery/dataset"
  storage_location = var.storage_location
  ads_report_dataset_id = var.ads_report_dataset_id
  config_dataset_id = var.config_dataset_id
  adh_branding_dataset_id = var.adh_branding_dataset_id
  adh_audience_dataset_id = var.adh_audience_dataset_id
  adh_lego_dataset_id = var.adh_lego_dataset_id

  depends_on = [module.storage]
}

module "bigquery_table" {
  source = "./bigquery/table"
  config_dataset_id = var.config_dataset_id
  fx_rate_spreadsheet_id = var.fx_rate_spreadsheet_id

  depends_on = [module.bigquery_dataset]
}

module "pubsub_topic" {
  source = "./pubsub/topic"
  namespace = var.namespace

  depends_on = [module.storage]
}

module "pubsub_subscription" {
  source = "./pubsub/subscription"
  region = var.region
  namespace = var.namespace
  project_id = var.project_id

  depends_on = [module.pubsub_topic]
}

module "logging_log_sink" {
  source = "./logging_log_sink"
  namespace = var.namespace
  project_id = var.project_id

  depends_on = [module.pubsub_subscription]
}

module "cloud_function" {
  source = "./cloud_function"
  namespace = var.namespace
  project_id = var.project_id
  region = var.region
  storage_location = var.storage_location
  tentacles_outbound = var.tentacles_outbound

  depends_on = [module.logging_log_sink]
}

module "cron_scheduler" {
  source = "./cloud_scheduler"
  namespace = var.namespace
  project_id = var.project_id
  timezone = var.timezone
  developer_token = var.developer_token
  ads_report_dataset_id = var.ads_report_dataset_id
  mcc_cids = var.mcc_cids
  adh_customer_id = var.adh_customer_id

  depends_on = [module.cloud_function]
}
