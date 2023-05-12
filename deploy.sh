#!/usr/bin/env bash
#
# Copyright 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Need to get shell lib files ready before import them.
npm install

# Cloud Functions Runtime Environment.
CF_RUNTIME=nodejs14

SOLUTION_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" -ef "$0" ]]; then
  RELATIVE_PATH="node_modules/@google-cloud"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/nodejs-common/bin/install_functions.sh"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/gmp-googleads-connector/deploy.sh"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/data-tasks-coordinator/deploy.sh"
fi

# ADH API version
ADH_API_VERSION=v1
# FX Rate Sheet
FX_RATE_SHEET=https://docs.google.com/spreadsheets/d/1WGmemVpB-qNRjQ8Sw2pm2TebEMh6OcF2fcAcXIo64qE/copy

# Project namespace will be used as prefix of the name of Cloud Functions,
# Pub/Sub topics, etc.
# Default project namespace is SOLUTION_NAME.
# Note: only lowercase letters, numbers and dashes(-) are allowed.
PROJECT_NAMESPACE="lego"
# Project configuration file.
CONFIG_FILE="./config/config.json"
TIMEZONE="Asia/Shanghai"
# Enabled API for Tentacles.
# Use this to create of topics and subscriptions.
SELECTED_APIS_CODES=("PB")
# Tentacles monitor folder.
OUTBOUND=outbound/
# The dataset name.
DATASET_ID="ads_reports_data_v4"
CONFIG_DATASET_ID="ads_report_configs"
ADH_CREATIVE_DS_ID="adh_apps_data"
ADH_BRANDING_DS_ID="adh_branding"
ADH_AUDIENCE_DS_ID="adh_audience"
REGION_FOR_DS="US"
# The Git commit id.
GIT_COMMIT_ID="$(git log -1 --pretty=format:'%H')"

# The global value for `validate_googleads_account` function to connect
# the active version Google Ads API.
GOOGLE_ADS_API_VERSION=13

# The main workflow that this instance will install. There are following
# available workflows:
# 1. App
# 2. NonApp
# 3. App + NonApp
INSTALLED_WORKFLOW=""
# Other functionality, e.g. ADH or Google Sheet, etc.
INSTALLED_ADH_CREATIVE_WORKFLOW="N"
INSTALLED_ADH_BRANDING_WORKFLOW="N"
INSTALLED_ADH_AUDIENCE_WORKFLOW="N"
INSTALLED_TRDPTY_TRIX_DATA="N"
INSTALLED_BACKFILL_WORKFLOW_TRIGGER="N"
INSTALLED_YOUTUBE_WORKFLOW="N"
INSTALLED_CPP_WORKFLOW="N"

# The map of task config files that will be installed.
declare -A TASK_CONFIGS
TASK_CONFIGS=(
  ["./config/task_base.json"]=true
  ["./config/workflow_template.json"]=true
  ["./config/task_app.json"]=false
  ["./config/task_nonapp.json"]=false
  ["./config/workflow_app_hourly.json"]=false
  ["./config/workflow_app_nonapp.json"]=false
  ["./config/workflow_app_nonapplite.json"]=false
  ["./config/workflow_app.json"]=false
  ["./config/workflow_nonapp.json"]=false
  ["./config/workflow_nonapplite.json"]=false
)

# Parameter name used by functions to load and save config.
CONFIG_ITEMS=(
  "PROJECT_NAMESPACE"
  "TIMEZONE"
  "REGION"
  "GCS_CONFIG_BUCKET"
  "GCS_BUCKET"
  "OUTBOUND"
  "GIT_COMMIT_ID"
  "DATASET_ID"
  "CONFIG_DATASET_ID"
  "ADH_CREATIVE_DS_ID"
  "ADH_BRANDING_DS_ID"
  "ADH_AUDIENCE_DS_ID"
  "DATASET_LOCATION"
  "INSTALLED_WORKFLOW"
  "INSTALLED_TRDPTY_TRIX_DATA"
  "INSTALLED_BACKFILL_WORKFLOW_TRIGGER"
  "INSTALLED_ADH_CREATIVE_WORKFLOW"
  "INSTALLED_ADH_BRANDING_WORKFLOW"
  "INSTALLED_ADH_AUDIENCE_WORKFLOW"
  "INSTALLED_YOUTUBE_WORKFLOW"
  "INSTALLED_CPP_WORKFLOW"
)

# Description of functionality.
INTEGRATION_APIS_DESCRIPTION=(
  "Google Ads Reports for App"
  "Google Ads Reports for NonApp"
  "Google Ads Reports for NonApp lite"
  "Ads Data Hub for App Creative"
  "Ads Data Hub for App Branding"
  "Ads Data Hub for Audience+"
  "BigQuery query external tables based on Google Sheet"
  "Google Ads Reports backfill for the past 90 days. Must select Google Ads \
Reports also."
  "LEGO Extension: YouTube Channel Analysis."
  "LEGO Extension: CPP"
)

# Build installed workflows map and set each value to false as default.
declare -A INSTALLED_LEGO_WORKFLOWS
INSTALLED_LEGO_WORKFLOWS=(
  ["App"]=false
  ["NonApp"]=false
  ["NonAppLite"]=false
)

# APIs need to be enabled if corresponding functionality are selected.
INTEGRATION_APIS=(
  "googleads.googleapis.com"
  "googleads.googleapis.com"
  "googleads.googleapis.com"
  "adsdatahub.googleapis.com"
  "adsdatahub.googleapis.com"
  "adsdatahub.googleapis.com"
  "drive.googleapis.com"
  "N/A"
  "youtube.googleapis.com"
  "N/A"
)

#######################################
# Extra setting up for the LEGO functionality.
# Globals:
#   INSTALLED_LEGO_WORKFLOWS
# Arguments:
#   None
#######################################
setup_functionality_for_installation() {
  case "${1}" in
  0)
    INSTALLED_LEGO_WORKFLOWS["App"]=true
    ;;
  1)
    INSTALLED_LEGO_WORKFLOWS["NonApp"]=true
    ;;
  2)
    INSTALLED_LEGO_WORKFLOWS["NonAppLite"]=true
    ;;
  3)
    # ADH creative workflow depends on lego app_trd_asset_perf_report,
    # report_base_campaign_conversion, and report_base_campaigns tables
    # which are created within Base and App workflow.
    INSTALLED_LEGO_WORKFLOWS["App"]=true
    INSTALLED_ADH_CREATIVE_WORKFLOW="Y"
    ;;
  4)
    INSTALLED_ADH_BRANDING_WORKFLOW="Y"
    ;;
  5)
    # ADH audience workflow depends on lego nonapp_trd_user_interest table,
    # which is created within NonApp workflow.
    INSTALLED_LEGO_WORKFLOWS["NonApp"]=true
    INSTALLED_ADH_AUDIENCE_WORKFLOW="Y"
    ;;
  6)
    INSTALLED_TRDPTY_TRIX_DATA="Y"
    ;;
  7)
    INSTALLED_BACKFILL_WORKFLOW_TRIGGER="Y"
    ;;
  8)
    INSTALLED_LEGO_WORKFLOWS["App"]=true
    INSTALLED_YOUTUBE_WORKFLOW="Y"
    ;;
  9)
    INSTALLED_CPP_WORKFLOW="Y"
    ;;
  *) ;;
  esac
}

#######################################
# Confirm LEGO functionality. This will update the API list that need to be
# enabled and the scope for OAuth authentication.
# Globals:
#   TASK_CONFIGS
#   INSTALLED_WORKFLOW
#   INSTALLED_LEGO_WORKFLOWS
# Arguments:
#   None
#######################################
confirm_functionality() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Selecting LEGO functionality..."
  confirm_apis "setup_functionality_for_installation"

  if ${INSTALLED_LEGO_WORKFLOWS["NonApp"]} ; then
    if ${INSTALLED_LEGO_WORKFLOWS["App"]} ; then
      INSTALLED_WORKFLOW="App + NonApp"
    else
      INSTALLED_WORKFLOW="NonApp"
    fi
  elif ${INSTALLED_LEGO_WORKFLOWS["NonAppLite"]} ; then
    if ${INSTALLED_LEGO_WORKFLOWS["App"]} ; then
      INSTALLED_WORKFLOW="App + NonAppLite"
    else
      INSTALLED_WORKFLOW="NonAppLite"
    fi
  elif ${INSTALLED_LEGO_WORKFLOWS["App"]} ; then
    INSTALLED_WORKFLOW="App"
  fi
}

#######################################
# Upload tasks configuration files to Cloud Firestore or Datastore.
# Globals:
#   None
# Arguments:
#   Array of task configuration files
#######################################
update_workflow_task() {
  local configs=("$@")
  for config in "${configs[@]}"; do
    update_task_config ${config}
    quit_if_failed $?
  done
}

#######################################
# Create or update a Cloud Scheduler Job which target Pub/Sub.
# Globals:
#   PROJECT_NAMESPACE
# Arguments:
#   Task Id
#   Cron time string
#   Message body
#   Cronjob name
#######################################
update_workflow_cronjob() {
  local task_id=$1
  local cron=$2
  local message_body=$3
  local cronjob_name=$4
  cronjob_name="${cronjob_name:-${PROJECT_NAMESPACE}-${task_id}}"
  create_or_update_cloud_scheduler_for_pubsub \
    ${cronjob_name} \
    "${cron}" \
    "${TIMEZONE}" \
    ${PROJECT_NAMESPACE}-monitor \
    "${message_body}" \
    taskId=${task_id}
}

#######################################
# Pause a Cloud Scheduler Job.
# Arguments:
#   The name of the Cloud Scheduler Job.
#######################################
pause_cloud_scheduler() {
  gcloud scheduler jobs pause $1
}

#######################################
# Validate whether the current OAuth token can access ADH account.
# Globals:
#   None
# Arguments:
#   ADH CID
#######################################
validate_adh_account() {
  local cid accessToken request response
  cid=${1}
  accessToken=$(get_oauth_access_token)
  request=(
    -H "Accept: application/json"
    -H "Content-Type: application/json"
    -H "Authorization: Bearer ${accessToken}"
    -s "https://adsdatahub.googleapis.com/${ADH_API_VERSION}/customers/${cid}"
  )
  response=$(curl "${request[@]}")
  local errorCode errorMessage
  errorCode=$(get_value_from_json_string "${response}" "error.code")
  if [[ -n "${errorCode}" ]]; then
    errorMessage=$(get_value_from_json_string "${response}" "error.message")
    printf '%s\n' "Validate failed: ${errorMessage}" >&2
    return 1
  fi
  return 0
}

#######################################
# Let user input ADH Customer ID for cronjob(s).
# Globals:
#   ADH_CID
# Arguments:
#   None
#######################################
set_adh_account() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Setting up Ads Data Hub account information..."
  while :; do
    printf '%s' "  Enter the ADH Customer ID: "
    read -r input
    if [[ -z ${input} ]]; then
      continue
    fi
    input="$(printf '%s' "${input}" | sed -r 's/-//g')"
    printf '%s' "    validating ${input}...... "
    validate_adh_account ${input}
    if [[ $? -eq 1 ]]; then
      printf '%s\n' "failed. Press 'C' to continue with this another CID or \
any other key to enter another CID..."
      local any
      read -n1 -s any
      if [[ "${any}" == "C" ]]; then
        printf '%s\n' "WARNING! Continue with FAILED CID ${input}."
      else
        continue
      fi
    else
      printf '%s\n' "succeeded."
    fi
    ADH_CID="${input}"
    printf '%s\n' "Using ADH Customer ID: ${ADH_CID}."
    break
  done
}

#######################################
# Initialize task configuration and Cloud Scheduler jobs for the selected
# workflow.
# Globals:
#   INSTALLED_WORKFLOW
#   INSTALLED_LEGO_WORKFLOWS
#   INSTALLED_TRDPTY_TRIX_DATA
#   INSTALLED_ADH_CREATIVE_WORKFLOW
#   INSTALLED_ADH_BRANDING_WORKFLOW
#   INSTALLED_ADH_AUDIENCE_WORKFLOW
#   INSTALLED_BACKFILL_WORKFLOW_TRIGGER
#   INSTALLED_YOUTUBE_WORKFLOW
#   INSTALLED_CPP_WORKFLOW
# Arguments:
#   Whether update cronjob.
#######################################
initialize_workflow() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Starting to initialize the workflow..."

  # Gathering information
  local flag=$1
  local updateCronjob=0
  if [[ ${flag,,} =~ "updatecron" ]]; then
    updateCronjob=1
  fi
  if [[ -z "${INSTALLED_WORKFLOW}" ]]; then
    confirm_functionality
  fi

  if [[ ${INSTALLED_WORKFLOW} = "App + NonApp" ]] ; then
    TASK_CONFIGS["./config/task_nonapp.json"]=true
    TASK_CONFIGS["./config/task_app.json"]=true
    TASK_CONFIGS["./config/workflow_app_nonapp.json"]=true
    TASK_CONFIGS["./config/workflow_app_hourly.json"]=true
  elif [[ ${INSTALLED_WORKFLOW} = "App + NonAppLite" ]] ; then
    TASK_CONFIGS["./config/task_nonapp.json"]=true
    TASK_CONFIGS["./config/task_app.json"]=true
    TASK_CONFIGS["./config/workflow_app_nonapplite.json"]=true
    TASK_CONFIGS["./config/workflow_app_hourly.json"]=true
  elif [[ ${INSTALLED_WORKFLOW} = "NonApp" ]] ; then
    TASK_CONFIGS["./config/task_nonapp.json"]=true
    TASK_CONFIGS["./config/workflow_nonapp.json"]=true
  elif [[ ${INSTALLED_WORKFLOW} = "NonAppLite" ]] ; then
    TASK_CONFIGS["./config/task_nonapp.json"]=true
    TASK_CONFIGS["./config/workflow_nonapplite.json"]=true
  elif [[ ${INSTALLED_WORKFLOW} = "App" ]] ; then
    TASK_CONFIGS["./config/task_app.json"]=true
    TASK_CONFIGS["./config/workflow_app_hourly.json"]=true
    TASK_CONFIGS["./config/workflow_app.json"]=true
  fi

  check_firestore_existence

  # Filter the required config files based on the selected lego workflows.
  local taskConfigs
  for config in "${!TASK_CONFIGS[@]}"; do
    if ${TASK_CONFIGS["$config"]}; then
      taskConfigs+=("$config")
    fi
  done

  # Create/update workflow task config and cronjob.
  update_workflow_task "${taskConfigs[@]}"
  if [[ ${updateCronjob} -eq 1 ]]; then
    if [[ -z "${MCC_CIDS}" || -z "${DEVELOPER_TOKEN}" ]]; then
      set_google_ads_account
    fi
    # Change MCC list string from comma separated into '\n' separated.
    local mccCids
    mccCids="$(printf '%s' "${MCC_CIDS}" | sed -r 's/,/\\\\n/g')"
    local message_body='{
      "timezone":"'"${TIMEZONE}"'",
      "partitionDay": "${today}",
      "datasetId": "'"${DATASET_ID}"'",
      "fromDate": "${today_sub_30_hyphenated}",
      "developerToken":"'"${DEVELOPER_TOKEN}"'",
      "mccCids": "'"${mccCids}"'"
    }'
    update_workflow_cronjob "lego_start" "0 6 * * *" "${message_body}"
    update_workflow_cronjob "lego_start_hourly" "0 7-23 * * *" \
      "${message_body}"
    pause_cloud_scheduler ${PROJECT_NAMESPACE}-lego_start_hourly

    # Create backfill cronjob.
    if [[ ${INSTALLED_BACKFILL_WORKFLOW_TRIGGER,,} = "y" ]]; then
      local backfill_message_body='{
        "timezone":"'"${TIMEZONE}"'",
        "partitionDay": "${today}",
        "datasetId": "'"${DATASET_ID}"'",
        "fromDate": "${today_sub_90_hyphenated}",
        "developerToken":"'"${DEVELOPER_TOKEN}"'",
        "mccCids": "'"${mccCids}"'"
      }'
      update_workflow_cronjob "lego_start" "0 12 1 7 *" \
        "${backfill_message_body}" "${PROJECT_NAMESPACE}-lego_start_backfill"
      pause_cloud_scheduler ${PROJECT_NAMESPACE}-lego_start_backfill
    fi
  fi

  # Create/update 3rd party data task config and cronjob.
  if [[ ${INSTALLED_TRDPTY_TRIX_DATA,,} = "y" ]]; then
    update_workflow_task "./config/task_trdpty.json"
    if [[ ${updateCronjob} -eq 1 ]]; then
      local message_body='{
        "timezone":"'"${TIMEZONE}"'",
        "partitionDay": "${today}",
        "datasetId": "'${DATASET_ID}'"
      }'
      update_workflow_cronjob "trdpty_load_reports" "0 7-23 * * *" \
        "${message_body}"
      pause_cloud_scheduler ${PROJECT_NAMESPACE}-trdpty_load_reports
    fi
  fi

  # Create/update ADH creative task config and cronjob.
  if [[ ${INSTALLED_ADH_CREATIVE_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_adh.json"
    if [[ ${updateCronjob} -eq 1 ]]; then
      if [[ -z "${ADH_CID}" ]]; then
        set_adh_account
      fi
      local message_body='{
        "timezone":"'"${TIMEZONE}"'",
        "partitionDay": "${today}",
        "legoDatasetId": "'"${DATASET_ID}"'",
        "adhCustomerId": "'"${ADH_CID}"'"
      }'
      update_workflow_cronjob "adh_lego_start" "0 13 * * 1" "${message_body}"
      pause_cloud_scheduler ${PROJECT_NAMESPACE}-adh_lego_start
    fi
  fi

  # Create/update ADH branding task config and cronjob.
  if [[ ${INSTALLED_ADH_BRANDING_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_adh_branding.json"
    if [[ ${updateCronjob} -eq 1 ]]; then
      if [[ -z "${ADH_CID}" ]]; then
        set_adh_account
      fi
      local message_body='{
        "timezone":"'"${TIMEZONE}"'",
        "partitionDay": "${today}",
        "adhCustomerId": "'"${ADH_CID}"'"
      }'
      update_workflow_cronjob "adh_branding_start" "0 12 1 1 *" "${message_body}"
      pause_cloud_scheduler ${PROJECT_NAMESPACE}-adh_branding_start
    fi
  fi

  # Create/update ADH audience task config and cronjob.
  if [[ ${INSTALLED_ADH_AUDIENCE_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_retail_adh.json"
    if [[ ${updateCronjob} -eq 1 ]]; then
      if [[ -z "${ADH_CID}" ]]; then
        set_adh_account
      fi
      local message_body='{
        "timezone":"'"${TIMEZONE}"'",
        "partitionDay": "${today}",
        "legoDatasetId": "'"${DATASET_ID}"'",
        "adhCustomerId": "'"${ADH_CID}"'"
      }'
      update_workflow_cronjob "adh_audience_start" "0 15 * * 1" "${message_body}"
      pause_cloud_scheduler ${PROJECT_NAMESPACE}-adh_audience_start
    fi
  fi

  # Create/update LEGO Youtube Extension.
  if [[ ${INSTALLED_YOUTUBE_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_youtube.json"
    if [[ ${updateCronjob} -eq 1 ]]; then
      local message_body='{
        "timezone": "'"${TIMEZONE}"'",
        "partitionDay": "${today}",
        "legoDatasetId": "'"${DATASET_ID}"'"
      }'
      update_workflow_cronjob "youtube_start" "0 15 * * *" "${message_body}"
    fi
  fi

  # Create/update LEGO CPP Extension for GrCN market.
  if [[ ${INSTALLED_CPP_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_cpp.json"
    if [[ -z "${MCC_CIDS}" || -z "${DEVELOPER_TOKEN}" ]]; then
      set_google_ads_account
    fi
    # Change MCC list string from comma separated into '\n' separated.
    local mccCids
    mccCids="$(printf '%s' "${MCC_CIDS}" | sed -r 's/,/\\\\n/g')"
    if [[ ${updateCronjob} -eq 1 ]]; then
      local message_body='{
        "timezone":"'"${TIMEZONE}"'",
        "partitionDay": "${today}",
        "datasetId": "'"${DATASET_ID}"'",
        "fromDate": "${today_sub_30_hyphenated}",
        "datasetId": "'"${DATASET_ID}"'",
        "developerToken":"'"${DEVELOPER_TOKEN}"'",
        "mccCids": "'"${mccCids}"'"
      }'
      update_workflow_cronjob "lego_cpp_start" "0 12 * * *" "${message_body}"
    fi
  fi
}

#######################################
# Confirm and create the Big Query dataset and GCS buckets base on the
# selected locations and name.
# Globals:
#   GCP_PROJECT
#   INSTALLED_WORKFLOW
#   INSTALLED_ADH_CREATIVE_WORKFLOW
#   INSTALLED_ADH_BRANDING_WORKFLOW
#   INSTALLED_ADH_AUDIENCE_WORKFLOW
#   DATASET_ID
#   CONFIG_DATASET_ID
#   ADH_CREATIVE_DS_ID
#   ADH_BRANDING_DS_ID
#   ADH_AUDIENCE_DS_ID
#   DATASET_LOCATION
#   REGION_FOR_DS
#   GCS_BUCKET
#   GCS_CONFIG_BUCKET
#   BUCKET_LOCATION
# Arguments:
#   None
#######################################
confirm_data_locations() {
  if [[ "${INSTALLED_WORKFLOW}" != "" ]]; then
    confirm_located_dataset DATASET_ID DATASET_LOCATION REGION_FOR_DS
    confirm_located_dataset CONFIG_DATASET_ID DATASET_LOCATION
  fi
  if [[ "${INSTALLED_ADH_CREATIVE_WORKFLOW}" == "Y" ]]; then
    confirm_located_dataset ADH_CREATIVE_DS_ID DATASET_LOCATION REGION_FOR_DS
  fi
  if [[ "${INSTALLED_ADH_BRANDING_WORKFLOW}" == "Y" ]]; then
    confirm_located_dataset ADH_BRANDING_DS_ID DATASET_LOCATION REGION_FOR_DS
  fi
  if [[ "${INSTALLED_ADH_AUDIENCE_WORKFLOW}" == "Y" ]]; then
    confirm_located_dataset ADH_AUDIENCE_DS_ID DATASET_LOCATION REGION_FOR_DS
  fi

  defaultBucketName=$(get_default_bucket_name "${GCP_PROJECT}")
  GCS_CONFIG_BUCKET="${defaultBucketName}-config"

  confirm_located_bucket GCS_BUCKET BUCKET_LOCATION DATASET_LOCATION REGION_FOR_DS
  confirm_located_bucket GCS_CONFIG_BUCKET BUCKET_LOCATION DATASET_LOCATION REGION_FOR_DS
}

#######################################
# Copy given folder to GCS config bucket.
# Globals:
#   GCS_CONFIG_BUCKET
# Arguments:
#   The name of the folder.
#######################################
copy_folder_to_gcs_config() {
  copy_to_gcs "${1}" "gs://${GCS_CONFIG_BUCKET}"
}

#######################################
# Set report lifecycle rule to GCS report bucket.
# Globals:
#   CONFIG_GCS_REPORT_LIFECYCLE_FILE
#   GCS_BUCKET
# Arguments:
#   None
#######################################
set_gcs_lifecycle() {
  gsutil lifecycle set /dev/stdin "gs://${GCS_BUCKET}" <<< '{"rule":[{"action":{"type":"Delete"},"condition":{"age":3}}]}'
}

#######################################
# Creates the BigQuery table for fx_rate_table.
# There are three steps:
# 1. Asks the user to make a copy of the Google Sheet by clicking a link;
# 2. The user clicks the menu in Google Sheet to register itself as en external
#    table in BigQuery. The user needs to complete anthroization and input the
#    Google Cloud Project in the Google Sheet.
# 3. The user add the Cloud Functions' service account to the Google Sheet with
#    Viewer access.
# Arguments:
#   N/A
#######################################
create_fx_rate_table() {
  # 1, copy the sheet
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Creating the FX rate table in BigQuery..."
  printf '%s\n' "  1. Click the link and make your own copy ${FX_RATE_SHEET}"
  printf '%s' "Press any key to continue after you have your copy..."
  local any
  read -n1 -s any
  printf '\n'
  # 2, register as external table
  printf '%s\n' "  2. Click menu of your Google Sheet with the name \
[LEGO] -> [Register as a BigQuery external table], enter the project id as \
"${GCP_PROJECT}" then click [OK]"
  printf '%s' "Press any key to continue after it completes..."
  local any
  read -n1 -s any
  printf '\n'
  # 3, add CF SA to the sheet
  local defaultServiceAccount=$(get_cloud_functions_service_account \
    "${PROJECT_NAMESPACE}_main")
  printf '%s\n' "  3. Open the Google Sheet and grant the Viewer \
access to service account: ${defaultServiceAccount}"
  printf '%s' "Press any key to continue after you grant the access..."
  local any
  read -n1 -s any
  printf '\n\n'
}

# Same tasks group for different installations.
COMMON_INSTALL_TASKS=(
  confirm_region
  enable_apis
  "confirm_firestore native"
  confirm_data_locations
  save_config
  create_subscriptions
  create_sink
  deploy_tentacles
  do_oauth
  deploy_sentinel
  set_internal_task
  "copy_folder_to_gcs_config sql"
  "copy_folder_to_gcs_config config"
  set_gcs_lifecycle
  "update_api_config ./config/config_api.json"
  create_fx_rate_table
  "initialize_workflow updateCronjob"
  "print_finished LEGO"
)

# Installation for default solution.
DEFAULT_INSTALL_TASKS=(
  "print_welcome LEGO"
  confirm_project
  check_permissions
  confirm_namespace
  confirm_functionality
  confirm_timezone
  "${COMMON_INSTALL_TASKS[@]}"
)

# Installation for specific solutions, no 'confirm_functionality' or
# 'confirm_timezone' step
CUSTOMIZED_INSTALL_TASKS=(
  "print_welcome LEGO"
  confirm_project
  check_permissions
  confirm_namespace
  "${COMMON_INSTALL_TASKS[@]}"
)

# Tasks for minimum interaction.
MINIMALISM_TASKS=(
  "print_welcome LEGO"
  confirm_project
  confirm_region
  "confirm_firestore native"
  confirm_data_locations
  save_config
  do_oauth
  enable_apis
  "initialize_workflow updateCronjob"
  create_subscriptions
  create_sink
  deploy_tentacles
  deploy_sentinel
  create_fx_rate_table
  set_internal_task
  "copy_folder_to_gcs_config sql"
  "copy_folder_to_gcs_config config"
  set_gcs_lifecycle
  "update_api_config ./config/config_api.json"
  "print_finished LEGO"
)

upgrade_cf() {
  ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adwords")
  confirm_region
  do_oauth
  deploy_sentinel
}

upgrade_cf_adh() {
  ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adsdatahub")
  upgrade_cf
}

setup_cn() {
  TIMEZONE="Asia/Shanghai"
  INSTALLED_TRDPTY_TRIX_DATA="Y"
  INSTALLED_ADH_CREATIVE_WORKFLOW="N"
  INSTALLED_ADH_BRANDING_WORKFLOW="N"
  INSTALLED_ADH_AUDIENCE_WORKFLOW="N"
  INSTALLED_BACKFILL_WORKFLOW_TRIGGER="N"
  INSTALLED_YOUTUBE_WORKFLOW="N"
  INSTALLED_CPP_WORKFLOW="N"
  GOOGLE_CLOUD_APIS["googleads.googleapis.com"]+="Google Ads API"
  ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adwords")
}

cn_app() {
  setup_cn
  INSTALLED_LEGO_WORKFLOWS["App"]=true
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_app_with_youtube() {
  setup_cn
  INSTALLED_LEGO_WORKFLOWS["App"]=true
  INSTALLED_YOUTUBE_WORKFLOW="Y"
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_nonapp() {
  setup_cn
  INSTALLED_LEGO_WORKFLOWS["NonApp"]=true
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_agency() {
  setup_cn
  INSTALLED_LEGO_WORKFLOWS["App"]=true
  INSTALLED_LEGO_WORKFLOWS["NonApp"]=true
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_adh_creative() {
  setup_cn
  INSTALLED_LEGO_WORKFLOWS["App"]=true
  INSTALLED_ADH_CREATIVE_WORKFLOW="Y"
  GOOGLE_CLOUD_APIS["adsdatahub.googleapis.com"]+="Ads Data Hub Queries"
  ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adsdatahub")
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_adh_branding() {
  setup_cn
  INSTALLED_ADH_BRANDING_WORKFLOW="Y"
  GOOGLE_CLOUD_APIS["adsdatahub.googleapis.com"]+="Ads Data Hub Queries"
  ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adsdatahub")
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_adh_audience() {
  setup_cn
  INSTALLED_LEGO_WORKFLOWS["NonApp"]=true
  INSTALLED_ADH_AUDIENCE_WORKFLOW="Y"
  GOOGLE_CLOUD_APIS["adsdatahub.googleapis.com"]+="Ads Data Hub Queries"
  ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adsdatahub")
  customized_install "${MINIMALISM_TASKS[@]}"
}

setup_au() {
  TIMEZONE="Australia/Sydney"
  INSTALLED_ADH_CREATIVE_WORKFLOW="N"
  INSTALLED_ADH_BRANDING_WORKFLOW="N"
  INSTALLED_ADH_AUDIENCE_WORKFLOW="N"
  INSTALLED_TRDPTY_TRIX_DATA="N"
  INSTALLED_BACKFILL_WORKFLOW_TRIGGER="N"
  TASK_CONFIGS["./config/task_customized_empty.json"]=true
  GOOGLE_CLOUD_APIS["googleads.googleapis.com"]+="Google Ads API"
  ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adwords")
}

# AU App: App, no ADH, no Sheet
au_app() {
  setup_au
  INSTALLED_LEGO_WORKFLOWS["App"]=true
  customized_install "${CUSTOMIZED_INSTALL_TASKS[@]}"
}

# AU Agency: App + NonApp, no ADH, no Sheet
au_agency() {
  setup_au
  INSTALLED_LEGO_WORKFLOWS["App"]=true
  INSTALLED_LEGO_WORKFLOWS["NonApp"]=true
  customized_install "${CUSTOMIZED_INSTALL_TASKS[@]}"
}

run_default_function "$@"
