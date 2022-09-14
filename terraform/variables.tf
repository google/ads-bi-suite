variable "project_id" {
    type = string
    default = "$your_gcp_project_id"
}

variable "developer_token" {
    type = string
    default = "$google_ads_developer_token"
}

variable "mcc_cids" {
    type = string
    default = "$your_google_ads_mcc_cids"
}

variable "adh_customer_id" {
    type = string
    default = "$your_adh_cid"
}

variable "fx_rate_spreadsheet_id" {
    type = string
    default = "$your_fx_rate_spreadsheet_id"
}

variable "namespace" {
    type = string
    default = "lego"
}

variable "region" {
    type = string
    default = "us-central1"
}

variable "storage_location" {
    type = string
    default = "US"
}

variable "tentacles_outbound" {
    type = string
    default = "outbound/"
}

variable "timezone" {
    type = string
    default = "Asia/Shanghai"
}

variable "ads_report_dataset_id" {
    type = string
    default = "ads_reports_data_v4"
}

variable "config_dataset_id" {
    type = string
    default = "ads_report_configs"
}

variable "adh_branding_dataset_id" {
    type = string
    default = "adh_branding"
}

variable "adh_audience_dataset_id" {
    type = string
    default = "adh_audience"
}

variable "adh_lego_dataset_id" {
    type = string
    default = "adh_apps_data"
}

variable "apis_map" {
    type = map
    description = "The api mapping between LEGO functions and gcloud APIs."
    default = {
        google_ads = ["googleads.googleapis.com"]
        ads_data_hub = ["adsdatahub.googleapis.com"]
    }
}

variable "lego_functions" {
    type = map
    description = "The api list between LEGO functions and gcloud APIs."
    default = {
        google_ads = true
        ads_data_hub = false
    }
}