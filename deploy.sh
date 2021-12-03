#!/usr/bin/env bash
#
# Copyright 2020 Google Inc.
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

# Google Ads API version
GOOGLE_ADS_API_VERSION=v7
# ADH API version
ADH_API_VERSION=v1

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
REGION_FOR_DS="US"

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

# The task config files that will be installed by default
DEFAULT_TASK_CONFIG=(
  "./config/task_base.json"
  "./config/workflow_template.json"
)

# Parameter name used by functions to load and save config.
CONFIG_ITEMS=(
  "PROJECT_NAMESPACE"
  "TIMEZONE"
  "REGION"
  "GCS_BUCKET"
  "OUTBOUND"
  "DATASET_ID"
  "CONFIG_DATASET_ID"
  "DATASET_LOCATION"
  "INSTALLED_WORKFLOW"
  "INSTALLED_TRDPTY_TRIX_DATA"
  "INSTALLED_BACKFILL_WORKFLOW_TRIGGER"
  "INSTALLED_ADH_CREATIVE_WORKFLOW"
  "INSTALLED_ADH_BRANDING_WORKFLOW"
  "INSTALLED_ADH_AUDIENCE_WORKFLOW"
  "INSTALLED_YOUTUBE_WORKFLOW"
)

# Description of functionality.
INTEGRATION_APIS_DESCRIPTION=(
  "Google Ads Reports for App"
  "Google Ads Reports for NonApp"
  "Ads Data Hub for App Creative"
  "Ads Data Hub for App Branding"
  "Ads Data Hub for Audience+"
  "BigQuery query external tables based on Google Sheet"
  "Google Ads Reports backfill for the past 90 days. Must select Google Ads \
Reports also."
  "LEGO Extension: YouTube Channel Analysis."
)

# APIs need to be enabled if corresponding functionality are selected.
INTEGRATION_APIS=(
  "googleads.googleapis.com"
  "googleads.googleapis.com"
  "adsdatahub.googleapis.com"
  "adsdatahub.googleapis.com"
  "adsdatahub.googleapis.com"
  "drive.googleapis.com"
  "N/A"
  "youtube.googleapis.com"
)

#######################################
# Extra setting up for the LEGO functionality.
# Globals:
#   None
# Arguments:
#   None
#######################################
setup_functionality_for_installation() {
  case "${1}" in
  0)
    if [[ -z "${INSTALLED_WORKFLOW}" ]]; then
      INSTALLED_WORKFLOW="App"
    elif [[ "${INSTALLED_WORKFLOW}" == "NonApp" ]]; then
      INSTALLED_WORKFLOW="App + NonApp"
    fi
    ;;
  1)
    if [[ -z "${INSTALLED_WORKFLOW}" ]]; then
      INSTALLED_WORKFLOW="NonApp"
    elif [[ "${INSTALLED_WORKFLOW}" == "App" ]]; then
      INSTALLED_WORKFLOW="App + NonApp"
    fi
    ;;
  2)
    INSTALLED_ADH_CREATIVE_WORKFLOW="Y"
    if [[ "${INSTALLED_WORKFLOW}" == "" ]]; then
      INSTALLED_WORKFLOW="App"
    fi
    ;;
  3)
    INSTALLED_ADH_BRANDING_WORKFLOW="Y"
    ;;
  4)
    INSTALLED_ADH_AUDIENCE_WORKFLOW="Y"
    if [[ "${INSTALLED_WORKFLOW}" == "" ]]; then
      INSTALLED_WORKFLOW="NonApp"
    fi
    ;;
  5)
    INSTALLED_TRDPTY_TRIX_DATA="Y"
    ;;
  6)
    INSTALLED_BACKFILL_WORKFLOW_TRIGGER="Y"
    ;;
  7)
    INSTALLED_YOUTUBE_WORKFLOW="Y"
    if [[ "${INSTALLED_WORKFLOW}" == "NonApp" ]]; then
      INSTALLED_WORKFLOW="App + NonApp"
    elif [[ "${INSTALLED_WORKFLOW}" == "" ]]; then
      INSTALLED_WORKFLOW="App"
    fi
    ;;
  *) ;;
  esac
}

#######################################
# Confirm LEGO functionality. This will update the API list that need to be
# enabled and the scope for OAuth authentication.
# Globals:
#   None
# Arguments:
#   None
#######################################
confirm_functionality() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Selecting LEGO functionality..."
  confirm_apis "setup_functionality_for_installation"
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
# Validate whether the current OAuth token, CID and developer token can work.
# Globals:
#   None
# Arguments:
#   MCC CID
#   Developer token
#######################################
validate_googleads_account() {
  local cid developerToken accessToken request response
  cid=${1}
  developerToken=${2}
  accessToken=$(get_oauth_access_token)
  request=(
    -H "Accept: application/json"
    -H "Content-Type: application/json"
    -H "developer-token: ${developerToken}"
    -H "Authorization: Bearer ${accessToken}"
    -s "https://googleads.googleapis.com/${GOOGLE_ADS_API_VERSION}/customers/${cid}"
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
# Let user input MCC CID and developer token for cronjob(s).
# Globals:
#   MCC_CIDS
#   DEVELOPER_TOKEN
# Arguments:
#   None
#######################################
set_google_ads_account() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Setting up Google Ads account information..."
  local developToken mccCids
  while :; do
    # Developer token
    while [[ -z ${developToken} ]]; do
      printf '%s' "  Enter the developer token[${DEVELOPER_TOKEN}]: "
      read -r input
      developToken="${input:-${DEVELOPER_TOKEN}}"
    done
    DEVELOPER_TOKEN="${developToken}"
    mccCids=""
    # MCC CIDs
    while :; do
      printf '%s' "  Enter the MCC CID: "
      read -r input
      if [[ -z ${input} ]]; then
        continue
      fi
      input="$(printf '%s' "${input}" | sed -r 's/-//g')"
      printf '%s' "    validating ${input}...... "
      validate_googleads_account ${input} ${DEVELOPER_TOKEN}
      if [[ $? -eq 1 ]]; then
        printf '%s\n' "failed.
      Press 'd' to re-enter developer token or
            'C' to continue with this MCC CID or
            any other key to enter another MCC CID..."
        local any
        read -n1 -s any
        if [[ "${any}" == "d" ]]; then
          developToken=""
          continue 2
        elif [[ "${any}" == "C" ]]; then
          printf '%s\n' "WARNING! Continue with FAILED MCC ${input}."
        else
          continue
        fi
      else
        printf '%s\n' "succeeded."
      fi
      mccCids+=",${input}"
      printf '%s' "  Do you want to add another MCC CID? [Y/n]: "
      read -r input
      if [[ ${input} == 'n' || ${input} == 'N' ]]; then
        break
      fi
    done
    # Left Shift one position to remove the first comma.
    # After shifting, MCC_CIDS would like "11111,22222".
    MCC_CIDS="${mccCids:1}"
    printf '%s\n' "Using Google Ads MCC CIDs: ${MCC_CIDS}."
    break
  done
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
#   INSTALLED_TRDPTY_TRIX_DATA
#   INSTALLED_ADH_CREATIVE_WORKFLOW
#   INSTALLED_ADH_BRANDING_WORKFLOW
#   INSTALLED_ADH_AUDIENCE_WORKFLOW
#   INSTALLED_BACKFILL_WORKFLOW_TRIGGER
#   INSTALLED_YOUTUBE_WORKFLOW
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

  check_firestore_existence
  # Preparing configuration based on workflow
  local taskConfigs
  taskConfigs=("${DEFAULT_TASK_CONFIG[@]}")

  case "${INSTALLED_WORKFLOW}" in
  "App")
    taskConfigs+=("./config/task_app.json")
    taskConfigs+=("./config/workflow_app.json")
    taskConfigs+=("./config/workflow_app_hourly.json")
    ;;
  "NonApp")
    taskConfigs+=("./config/task_nonapp.json")
    taskConfigs+=("./config/workflow_nonapp.json")
    ;;
  "App + NonApp")
    taskConfigs+=("./config/task_app.json")
    taskConfigs+=("./config/task_nonapp.json")
    taskConfigs+=("./config/workflow_app_nonapp.json")
    taskConfigs+=("./config/workflow_app_hourly.json")
    ;;
  *) ;;
  esac

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
      update_workflow_cronjob "adh_branding_start" "0 11 * * 1" "${message_body}"
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
}

# Same tasks group for different installations.
COMMON_INSTALL_TASKS=(
  confirm_region
  enable_apis
  "confirm_located_dataset DATASET_ID DATASET_LOCATION REGION_FOR_DS"
  "confirm_located_dataset CONFIG_DATASET_ID DATASET_LOCATION"
  "confirm_located_bucket GCS_BUCKET BUCKET_LOCATION DATASET_LOCATION"
  save_config
  create_subscriptions
  create_sink
  deploy_tentacles
  do_oauth
  deploy_sentinel
  set_internal_task
  copy_sql_to_gcs
  "update_api_config ./config/config_api.json"
  "initialize_workflow updateCronjob"
  "print_finished LEGO"
)

# Installation for default solution.
DEFAULT_INSTALL_TASKS=(
  "print_welcome LEGO"
  check_in_cloud_shell
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
  check_in_cloud_shell
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
  "confirm_located_dataset DATASET_ID DATASET_LOCATION REGION_FOR_DS"
  "confirm_located_dataset CONFIG_DATASET_ID DATASET_LOCATION"
  "confirm_located_bucket GCS_BUCKET BUCKET_LOCATION DATASET_LOCATION"
  save_config
  do_oauth
  enable_apis
  "initialize_workflow updateCronjob"
  create_subscriptions
  create_sink
  deploy_tentacles
  deploy_sentinel
  set_internal_task
  copy_sql_to_gcs
  "update_api_config ./config/config_api.json"
  "print_finished LEGO"
)

setup_cn() {
  TIMEZONE="Asia/Shanghai"
  INSTALLED_TRDPTY_TRIX_DATA="Y"
  INSTALLED_ADH_CREATIVE_WORKFLOW="N"
  INSTALLED_ADH_BRANDING_WORKFLOW="N"
  INSTALLED_ADH_AUDIENCE_WORKFLOW="N"
  INSTALLED_BACKFILL_WORKFLOW_TRIGGER="N"
  INSTALLED_YOUTUBE_WORKFLOW="N"
  GOOGLE_CLOUD_APIS["googleads.googleapis.com"]+="Google Ads API"
  ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adwords")
}

cn_app() {
  setup_cn
  INSTALLED_WORKFLOW="App"
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_app_with_youtube() {
  setup_cn
  INSTALLED_WORKFLOW="App"
  INSTALLED_YOUTUBE_WORKFLOW="Y"
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_nonapp() {
  setup_cn
  INSTALLED_WORKFLOW="NonApp"
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_agency() {
  setup_cn
  INSTALLED_WORKFLOW="App + NonApp"
  customized_install "${MINIMALISM_TASKS[@]}"
}

cn_adh_creative() {
  setup_cn
  INSTALLED_WORKFLOW="App"
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
  INSTALLED_WORKFLOW="NonApp"
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
  DEFAULT_TASK_CONFIG+=("./config/task_customized_empty.json")
  GOOGLE_CLOUD_APIS["googleads.googleapis.com"]+="Google Ads API"
  ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adwords")
}

# AU App: App, no ADH, no Sheet
au_app() {
  setup_au
  INSTALLED_WORKFLOW="App"
  customized_install "${CUSTOMIZED_INSTALL_TASKS[@]}"
}

# AU Agency: App + NonApp, no ADH, no Sheet
au_agency() {
  setup_au
  INSTALLED_WORKFLOW="App + NonApp"
  customized_install "${CUSTOMIZED_INSTALL_TASKS[@]}"
}

run_default_function "$@"
