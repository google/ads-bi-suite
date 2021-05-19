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

# Parameter to record the installed workflow version,
# which will be selected in task_config_manager().
INSTALLED_WORKFLOW_VERSION=""

# Parameter to record if user choose to install the experimental task,
# third party trix data, which will be asked in
# select_install_trdpty_trix_data().
INSTALLED_TRDPTY_TRIX_DATA="Y"

# Parameter name used by functions to load and save config. yest
CONFIG_FOLDER_NAME="OUTBOUND"
CONFIG_ITEMS=(
  "PROJECT_NAMESPACE"
  "GCS_BUCKET"
  "INSTALLED_WORKFLOW_VERSION"
  "INSTALLED_TRDPTY_TRIX_DATA"
  "${CONFIG_FOLDER_NAME}"
)

# Google Ads API enabled in this solution.
ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adwords")
GOOGLE_CLOUD_APIS["googleads.googleapis.com"]+="Google Ads API"
# Enabled API for Tentacles.
# Use this to create of topics and subscriptions.
SELECTED_APIS_CODES=("PB")
# Tentacles monitor folder.
OUTBOUND=outbound/

#TODO(jerry): Check whether the function 'initialize_workflow' can replace this.
#If yes, delete this function.
create_cron_job_for_lego_start() {
  _pause_cloud_scheduler() {
    gcloud scheduler jobs pause $1
  }

  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Starting to create or update Cloud Scheduler \
jobs for Google Ads reports..."
  local mcc_cid=$1
  local developer_token=$2
  while [[ -z ${mcc_cid} ]]; do
    printf '%s' "Enter the MCC CID: "
    read -r input
    mcc_cid=${input}
    printf '\n'
  done
  while [[ -z ${developer_token} ]]; do
    printf '%s' "Enter the developer token: "
    read -r input
    developer_token=${input}
    printf '\n'
  done
  local message_body='{
    "timezone":"'"${TIMEZONE}"'",
    "partitionDay": "${today}",
    "developerToken":"'${developer_token}'",
    "mccCid":"'${mcc_cid}'"
  }'
  local task_id="lego_start"
  local job_name=${PROJECT_NAMESPACE}-${task_id}
  create_or_update_cloud_scheduler_for_pubsub \
    ${job_name} \
    "0 6 * * *" \
    "${TIMEZONE}" \
    ${PROJECT_NAMESPACE}-monitor \
    "${message_body}" \
    taskId=${task_id}

  # Currently, There is no needed hourly workflow for NonApp.
  if [ "${INSTALLED_WORKFLOW_VERSION}" != "NonApp" ]; then
    task_id="lego_start_hourly"
    job_name=${PROJECT_NAMESPACE}-${task_id}
    create_or_update_cloud_scheduler_for_pubsub \
      ${job_name} \
      "0 7-23 * * *" \
      "${TIMEZONE}" \
      ${PROJECT_NAMESPACE}-monitor \
      "${message_body}" \
      taskId=${task_id}
  fi

  if [[ ${INSTALLED_TRDPTY_TRIX_DATA} = "Y" || ${INSTALLED_TRDPTY_TRIX_DATA} = "y" ]]; then
    local task_id="trdpty_load_reports"
    local job_name=${PROJECT_NAMESPACE}-${task_id}
    create_or_update_cloud_scheduler_for_pubsub \
      ${job_name} \
      "0 7-23 * * *" \
      "${TIMEZONE}" \
      ${PROJECT_NAMESPACE}-monitor \
      "${message_body}" \
      taskId=${task_id}

    _pause_cloud_scheduler ${job_name}
  fi
}

#TODO: add comments
select_installed_workflow_version() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Selecting the task configurations."

  local versions=("App" "NonApp" "App + NonApp")
  local greeting="Enter the number of the task configurations that you want to install:"

  # Install different workflow and related tasks config.
  printf "${greeting}\n"
  select version in "${versions[@]}"; do
    case "${version}" in
    "App")
      INSTALLED_WORKFLOW_VERSION="${version}"
      break
      ;;
    "NonApp")
      INSTALLED_WORKFLOW_VERSION="${version}"
      break
      ;;
    "App + NonApp")
      INSTALLED_WORKFLOW_VERSION="${version}"
      break
      ;;
    *)
      printf "${greeting}\n"
      ;;
    esac
  done
  printf "Finished; Close the interactive console.\n"
}

#TODO: add comments
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

#TODO(jerry): Check whether the function 'initialize_workflow' can replace this.
#If yes, delete this function.
task_config_manager() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Starting to install the task configurations."

  _install() {
    update_task_config $1
    quit_if_failed $?
  }

  if [ -z "${INSTALLED_WORKFLOW_VERSION}" ]; then
    select_installed_workflow_version
  fi

  # Install the common config files.
  _install "${SOLUTION_ROOT}/config/task_trdpty.json"
  _install "${SOLUTION_ROOT}/config/task_base.json"
  _install "${SOLUTION_ROOT}/config/task_app.json"
  _install "${SOLUTION_ROOT}/config/task_nonapp.json"
  _install "${SOLUTION_ROOT}/config/workflow_template.json"

  # Install different workflow and related tasks config.
  case "${INSTALLED_WORKFLOW_VERSION}" in
  "App")
    _install "${SOLUTION_ROOT}/config/workflow_app.json"
    _install "${SOLUTION_ROOT}/config/workflow_app_hourly.json"
    ;;
  "NonApp")
    _install "${SOLUTION_ROOT}/config/workflow_nonapp.json"
    ;;
  "App + NonApp")
    _install "${SOLUTION_ROOT}/config/workflow_app_nonapp.json"
    # Install app hourly workflow to fetch app related data hourly.
    _install "${SOLUTION_ROOT}/config/workflow_app_hourly.json"
    ;;
  *) ;;
  esac
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
# Create or update a Cloud Schedular Job which target Pub/Sub.
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
#   INSTALLED_WORKFLOW_VERSION
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
  if [[ -z "${INSTALLED_WORKFLOW_VERSION}" ]]; then
    select_installed_workflow_version
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
    "mccCid":"'${MCC_CID}'"
  }'
  local taskConfigs needHourly
  taskConfigs=(
    "./config/task_base.json"
    "./config/workflow_template.json"
  )
  case "${INSTALLED_WORKFLOW_VERSION}" in
  "App")
    taskConfigs+=("./config/task_app.json")
    taskConfigs+=("./config/workflow_app.json")
    needHourly="true"
    ;;
  "NonApp")
    taskConfigs+=("./config/task_nonapp.json")
    taskConfigs+=("./config/workflow_nonapp.json")
    needHourly="false"
    ;;
  "App + NonApp")
    taskConfigs+=("./config/task_app.json")
    taskConfigs+=("./config/task_nonapp.json")
    taskConfigs+=("./config/workflow_app_nonapp.json")
    needHourly="true"
    ;;
  *) ;;
  esac

  # Create/update workflow task config and cronjob.
  update_workflow_task "${taskConfigs[@]}"
  if [[ ${updateCronjob} -eq 1 ]]; then
    update_workflow_cronjob "lego_start" "0 6 * * *" "${message_body}"
  fi

  # Create/update hourly task config and cronjob.
  if [[ ${needHourly} = "true" ]]; then
    update_workflow_task "./config/workflow_app_hourly.json"
    if [[ ${updateCronjob} -eq 1 ]]; then
      update_workflow_cronjob "lego_start_hourly" "0 7-23 * * *" \
        "${message_body}"
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
      # Put it here until second case pop up and then reuse it.
      gcloud scheduler jobs pause ${PROJECT_NAMESPACE}-trdpty_load_reports
    fi
  fi
}

# Install
#TODO(jerry): If 'initialize_workflow' is adopted, change the tasks here.
DEFAULT_INSTALL_TASKS=(
  "print_welcome LEGO"
  check_in_cloud_shell
  prepare_dependencies
  confirm_namespace confirm_project
  check_permissions_native enable_apis
  confirm_region
  create_bucket confirm_folder
  select_installed_workflow_version
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
  "update_api_config ${SOLUTION_ROOT}/config/config_api.json"
  task_config_manager
  # Add MCC_CID and DEVELOPER_TOKEN as args for next function can create cronjob directly.
  # like this: "create_cron_job_for_lego_start 12345678  XXYYZZ_A_FAKE_DEV_TOKEN"
  create_cron_job_for_lego_start
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
  INSTALLED_WORKFLOW_VERSION="App"
  customized_install "${CUSTOMIZED_INSTALL_TASKS[@]}"
}

# Tasks for minimum interaction.
MINIMALISM_TASKS=(
  "print_welcome LEGO"
  confirm_project
  confirm_region
  create_bucket
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
  INSTALLED_WORKFLOW_VERSION="App"
  customized_install "${MINIMALISM_TASKS[@]}"
}

quick_nonapp() {
  INSTALLED_TRDPTY_TRIX_DATA="Y"
  INSTALLED_WORKFLOW_VERSION="NonApp"
  customized_install "${MINIMALISM_TASKS[@]}"
}

quick_all() {
  INSTALLED_TRDPTY_TRIX_DATA="Y"
  INSTALLED_WORKFLOW_VERSION="App + NonApp"
  customized_install "${MINIMALISM_TASKS[@]}"
}

run_default_function "$@"
