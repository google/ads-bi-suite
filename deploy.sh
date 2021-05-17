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
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/gmp-googleads-connector/deploy.sh"
  source "${SOLUTION_ROOT}/${RELATIVE_PATH}/data-tasks-coordinator/deploy.sh"
fi

# Project namespace will be used as prefix of the name of Cloud Functions,
# Pub/Sub topics, etc.
# Default project namespace is SOLUTION_NAME.
# Note: only lowercase letters, numbers and dashes(-) are allowed.
PROJECT_NAMESPACE="lego"
# Project configuration file.
CONFIG_FILE="${SOLUTION_ROOT}/config/config.json"
TIMEZONE="Asia/Shanghai"

# Parameter to record the installed workflow version,
# which will be selected in task_config_manager().
INSTALLED_WORKFLOW_VERSION=""

# Parameter to record if user choose to install the experimental task,
# third party trix data, which will be asked in
# select_install_trdpty_trix_data().
INSTALLED_TRDPTY_TRIX_DATA="Y"

# Parameter name used by functions to load and save config.
CONFIG_FOLDER_NAME="OUTBOUND"
CONFIG_ITEMS=("PROJECT_NAMESPACE" "GCS_BUCKET" "INSTALLED_WORKFLOW_VERSION" "INSTALLED_TRDPTY_TRIX_DATA" "${CONFIG_FOLDER_NAME}")
# Google Ads API enabled in this solution.
ENABLED_OAUTH_SCOPES+=("https://www.googleapis.com/auth/adwords")
GOOGLE_CLOUD_APIS["googleads.googleapis.com"]+="Google Ads API"
# Enabled API for Tentacles.
# Use this to create of topics and subscriptions.
SELECTED_APIS_CODES=("PB")

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

select_installed_workflow_version() {
  ((STEP += 1))
  printf '%s\n' "Step ${STEP}: Selecting the task configurations."

  local versions=("App" "NonApp" "App + NonApp")
  local greeting="Please select the number of the task configurations that you want to install."

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
    break
    ;;
  "NonApp")
    _install "${SOLUTION_ROOT}/config/workflow_nonapp.json"
    break
    ;;
  "App + NonApp")
    _install "${SOLUTION_ROOT}/config/workflow_app_nonapp.json"
    # Install app hourly workflow to fetch app related data hourly.
    _install "${SOLUTION_ROOT}/config/workflow_app_hourly.json"
    break
    ;;
  *) ;;
  esac
}

# Install
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

run_default_function "$@"
