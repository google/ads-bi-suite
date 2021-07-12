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

SOLUTION_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ "${BASH_SOURCE[0]}" -ef "$0" ]]; then
  RELATIVE_PATH="node_modules/@google-cloud"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/nodejs-common/bin/install_functions.sh"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/nodejs-common/bin/bigquery.sh"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/gmp-googleads-connector/deploy.sh"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/data-tasks-coordinator/deploy.sh"
fi

# Project namespace will be used as prefix of the name of Cloud Functions,
# Pub/Sub topics, etc.
# Default project namespace is SOLUTION_NAME.
# Note: only lowercase letters, numbers and dashes(-) are allowed.
PROJECT_NAMESPACE="lego"
# Project configuration file.
CONFIG_FILE="./config/config.json"
TIMEZONE="Asia/Shanghai"

# The dataset name.
DATASET_ID="ads_reports_data_v4"
DATASET_LOCATION="US"

# Parameter to record the installed workflow,
# which will be selected in select_workflow().
INSTALLED_WORKFLOW=""

# Parameter to record if user choose to install the experimental task,
# third party trix data, which will be asked in
# select_install_trdpty_trix_data().
INSTALLED_TRDPTY_TRIX_DATA="Y"

# Parameter to record if user choose to install the APP related workflow,
# which will be asked in select_install_app_related_workflow().
INSTALLED_APP_HOURLY_WORKFLOW="N"
INSTALLED_ADH_CREATIVE_WORKFLOW="N"

# Parameter name used by functions to load and save config.
CONFIG_FOLDER_NAME="OUTBOUND"
CONFIG_ITEMS=(
  "PROJECT_NAMESPACE"
  "GCS_BUCKET"
  "DATASET_ID"
  "INSTALLED_WORKFLOW"
  "INSTALLED_TRDPTY_TRIX_DATA"
  "INSTALLED_APP_HOURLY_WORKFLOW"
  "INSTALLED_ADH_CREATIVE_WORKFLOW"
  "${CONFIG_FOLDER_NAME}"
)

# Google Ads and ADH API enabled in this solution.
ENABLED_OAUTH_SCOPES+=(
  "https://www.googleapis.com/auth/adwords"
  "https://www.googleapis.com/auth/adsdatahub"
)

GOOGLE_CLOUD_APIS["googleads.googleapis.com"]+="Google Ads API"
# Enabled API for Tentacles.
# Use this to create of topics and subscriptions.
SELECTED_APIS_CODES=("PB")
# Tentacles monitor folder.
OUTBOUND=outbound/

#######################################
# Select the workflow version. There are three major workflows now,
# App, NonApp, and App + NonApp.
# Globals:
#   INSTALLED_WORKFLOW
#   INSTALLED_APP_HOURLY_WORKFLOW
#   INSTALLED_ADH_CREATIVE_WORKFLOW
# Arguments:
#   None
#######################################
select_workflow() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Selecting the workflows."

  local workflows=("App" "NonApp" "App + NonApp")
  local greeting="Enter the number of the workflow that you want to install:"

  # Install different workflow and related tasks config.
  printf '%s\n' "${greeting}"
  select workflow in "${workflows[@]}"; do
    case "${workflow}" in
    "App")
      INSTALLED_WORKFLOW="${workflow}"
      select_install_app_related_workflow
      break
      ;;
    "NonApp")
      INSTALLED_WORKFLOW="${workflow}"
      break
      ;;
    "App + NonApp")
      INSTALLED_WORKFLOW="${workflow}"
      select_install_app_related_workflow
      break
      ;;
    *)
      printf '%s\n' "${greeting}"
      ;;
    esac
  done
}

# TODO(chjerry): Rename the function name and related task configs after
# finalizing the specific use case. Current naming is too general to understand
# the perpose of this function.
#######################################
# Whether install the third party workflow to pull Google Sheet content.
# Globals:
#   INSTALLED_TRDPTY_TRIX_DATA
# Arguments:
#   None
#######################################
select_install_trdpty_trix_data() {
  ((STEP += 1))

  printf '%s\n' "Step ${STEP}: Do you want to install task\
, 3rd-Party Trix Data? [Y/n]: "
  local confirm_install
  read -r confirm_install
  if [[ ${confirm_install} = "N" || ${confirm_install} = "n" ]]; then
    INSTALLED_TRDPTY_TRIX_DATA="N"
  fi
}

#######################################
# Whether install the APP related and extended workflows.
# Globals:
#   INSTALLED_APP_HOURLY_WORKFLOW
#   INSTALLED_ADH_CREATIVE_WORKFLOW
# Arguments:
#   None
#######################################
select_install_app_related_workflow() {
  printf '%s\n' "Do you want to install APP hourly workflow? [Y/n]: "
  local confirm_install
  read -r confirm_install
  if [[ -z ${confirm_install} || ${confirm_install} = "Y" || ${confirm_install} = "y" ]]; then
    INSTALLED_APP_HOURLY_WORKFLOW="Y"
  fi

  printf '%s\n' "Do you want to install ADH weekly workflow? [Y/n]: "
  read -r confirm_install
  if [[ -z ${confirm_install} || ${confirm_install} = "Y" || ${confirm_install} = "y" ]]; then
    INSTALLED_ADH_CREATIVE_WORKFLOW="Y"
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
#######################################
update_workflow_cronjob() {
  local task_id=$1
  local cron=$2
  local message_body=$3
  create_or_update_cloud_scheduler_for_pubsub \
    ${PROJECT_NAMESPACE}-${task_id} \
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
# Let user input MCC CID and developer token for cronjob(s).
# Globals:
#   MCC_CID
#   DEVELOPER_TOKEN
# Arguments:
#   None
#######################################
set_google_ads_account() {
  while [[ -z ${MCC_CID} ]]; do
    printf '%s' "Enter the MCC CID: "
    read -r input
    MCC_CID=${input}
    printf '\n'
  done
  while [[ -z ${DEVELOPER_TOKEN} ]]; do
    printf '%s' "Enter the developer token: "
    read -r input
    DEVELOPER_TOKEN=${input}
    printf '\n'
  done
}

#######################################
# Initialize task configuration and Cloud Scheduler jobs for the selected
# workflow.
# Globals:
#   INSTALLED_WORKFLOW
#   INSTALLED_TRDPTY_TRIX_DATA
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
    select_workflow
  fi
  if [[ ${updateCronjob} -eq 1 ]]; then
    if [[ -z "${MCC_CID}" || -z "${DEVELOPER_TOKEN}" ]]; then
      set_google_ads_account
    fi
  fi

  # Preparing configuration based on workflow
  local message_body='{
    "timezone":"'"${TIMEZONE}"'",
    "partitionDay": "${today}",
    "developerToken":"'${DEVELOPER_TOKEN}'",
    "mccCid":"'${MCC_CID}'",
    "datasetId": "'${DATASET_ID}'"
  }'
  local taskConfigs
  taskConfigs=(
    "./config/task_base.json"
    "./config/workflow_template.json"
  )
  case "${INSTALLED_WORKFLOW}" in
  "App")
    taskConfigs+=("./config/task_app.json")
    taskConfigs+=("./config/workflow_app.json")
    ;;
  "NonApp")
    taskConfigs+=("./config/task_nonapp.json")
    taskConfigs+=("./config/workflow_nonapp.json")
    ;;
  "App + NonApp")
    taskConfigs+=("./config/task_app.json")
    taskConfigs+=("./config/task_nonapp.json")
    taskConfigs+=("./config/workflow_app_nonapp.json")
    ;;
  *) ;;
  esac

  # Create/update workflow task config and cronjob.
  update_workflow_task "${taskConfigs[@]}"
  if [[ ${updateCronjob} -eq 1 ]]; then
    update_workflow_cronjob "lego_start" "0 6 * * *" "${message_body}"
  fi

  # Create/update APP hourly task config and cronjob.
  if [[ ${INSTALLED_APP_HOURLY_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_app_hourly.json"
    if [[ ${updateCronjob} -eq 1 ]]; then
      update_workflow_cronjob "lego_start_hourly" "0 7-23 * * *" \
        "${message_body}"
      pause_cloud_scheduler ${PROJECT_NAMESPACE}-lego_start_hourly
    fi
  fi

  # Create/update 3rd party data task config and cronjob.
  if [[ ${INSTALLED_TRDPTY_TRIX_DATA,,} = "y" ]]; then
    update_workflow_task "./config/task_trdpty.json"
    if [[ ${updateCronjob} -eq 1 ]]; then
      # TODO: copied time setting and message body from previous version, but \
      # they don't make too much sense here. Please consider simplifying them.
      update_workflow_cronjob "trdpty_load_reports" "0 7-23 * * *" \
        "${message_body}"
      pause_cloud_scheduler ${PROJECT_NAMESPACE}-trdpty_load_reports
    fi
  fi

  # Create/update ADH creative task config and cronjob.
  if [[ ${INSTALLED_ADH_CREATIVE_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_adh.json"
    if [[ ${updateCronjob} -eq 1 ]]; then
      update_workflow_cronjob "adh_lego_start" "0 9 * * 0" \
        "${message_body}"
      pause_cloud_scheduler ${PROJECT_NAMESPACE}-adh_lego_start
    fi
  fi
}

# Tasks for the default installation.
DEFAULT_INSTALL_TASKS=(
  "print_welcome LEGO"
  check_in_cloud_shell
  confirm_namespace
  confirm_project
  check_permissions_native
  enable_apis
  confirm_region
  create_bucket
  confirm_folder
  "confirm_dataset_with_location DATASET_ID ${DATASET_LOCATION}"
  select_workflow
  select_install_trdpty_trix_data
  save_config
  check_firestore_existence
  create_subscriptions
  create_sink
  deploy_tentacles
  do_oauth
  deploy_cloud_functions_task_coordinator
  copy_sql_to_gcs
  set_internal_task
  "update_api_config ./config/config_api.json"
  "initialize_workflow updateCronjob"
  "print_finished LEGO"
)

# Tasks for detailed cases (workflows)
# There are some repeated tasks across different lists.
# TODO(lushu): Assess whether or not to reduce this dupalication.
CUSTOMIZED_INSTALL_TASKS=(
  "print_welcome LEGO"
  check_in_cloud_shell
  confirm_namespace confirm_project
  check_permissions enable_apis
  confirm_region
  create_bucket
  "confirm_dataset_with_location DATASET_ID ${DATASET_LOCATION}"
  save_config
  check_firestore_existence
  create_subscriptions
  create_sink
  deploy_tentacles
  do_oauth
  deploy_cloud_functions_task_coordinator
  copy_sql_to_gcs
  set_internal_task
  "update_api_config ./config/config_api.json"
  "initialize_workflow updateCronjob"
  "print_finished LEGO"
)

app_au() {
  TIMEZONE="Australia/Sydney"
  INSTALLED_TRDPTY_TRIX_DATA="N"
  INSTALLED_WORKFLOW="App"
  customized_install "${CUSTOMIZED_INSTALL_TASKS[@]}"
}

# Tasks for minimum interaction.
MINIMALISM_TASKS=(
  "print_welcome LEGO"
  confirm_project
  confirm_region
  create_bucket
  "confirm_dataset_with_location DATASET_ID ${DATASET_LOCATION}"
  save_config
  do_oauth
  check_firestore_existence
  set_google_ads_account
  enable_apis
  create_subscriptions
  create_sink
  deploy_tentacles
  deploy_cloud_functions_task_coordinator
  copy_sql_to_gcs
  set_internal_task
  "update_api_config ./config/config_api.json"
  "initialize_workflow updateCronjob"
  "print_finished LEGO"
)

quick_app() {
  INSTALLED_TRDPTY_TRIX_DATA="Y"
  INSTALLED_WORKFLOW="App"
  customized_install "${MINIMALISM_TASKS[@]}"
}

quick_nonapp() {
  INSTALLED_TRDPTY_TRIX_DATA="Y"
  INSTALLED_WORKFLOW="NonApp"
  customized_install "${MINIMALISM_TASKS[@]}"
}

quick_all() {
  INSTALLED_TRDPTY_TRIX_DATA="Y"
  INSTALLED_WORKFLOW="App + NonApp"
  customized_install "${MINIMALISM_TASKS[@]}"
}

run_default_function "$@"
