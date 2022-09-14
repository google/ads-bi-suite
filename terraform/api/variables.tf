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