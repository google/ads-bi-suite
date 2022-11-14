# Automatic Deployment Bash Script
# To deploy manually, please follow the LEGO deployment guide.

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
SCOPE="https://www.googleapis.com/auth/adwords"

# # The map of task config files that will be installed.
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

### Configuration variables in Pangu Start.
# Pub/Sub topics, etc.
# Default project namespace is SOLUTION_NAME.
# Note: only lowercase letters, numbers and dashes(-) are allowed.
PROJECT_NAMESPACE="lego"
GCS_BUCKET="${PROJECT_NAMESPACE}-${GCP_PROJECT}"
GCS_CONFIG_BUCKET="${PROJECT_NAMESPACE}-${GCP_PROJECT}-config"

# REGION="us-central1"
# REGION_FOR_DS="us"
# GCP_PROJECT="your_gcp_project_id"
# # The main workflow that this instance will install. There are following
# # available workflows:
# # 1. App
# # 2. NonApp
# # 3. NonAppLite
# # 4. App + NonApp
# # 5. App + NonAppLite
# INSTALLED_WORKFLOW="App + NonApp"
# # Other functionality, e.g. ADH or Google Sheet, etc.
# INSTALLED_ADH_CREATIVE_WORKFLOW="N"
# INSTALLED_ADH_BRANDING_WORKFLOW="N"
# INSTALLED_ADH_AUDIENCE_WORKFLOW="N"
# INSTALLED_TRDPTY_TRIX_DATA="N"
# INSTALLED_BACKFILL_WORKFLOW_TRIGGER="N"
# INSTALLED_CPP_WORKFLOW="N"

# DEVELOPER_TOKEN="your_ads_developer_token"
# MCC_CIDS="your_ads_cids_separate_with_\\n"
# ADH_CID="your_adh_cid"
# CLIENT_ID="your_oauth_client_id"
# CLIENT_SECRET="your_oauth_client_secret"
# REFRESH_TOKEN="your_oauth_refresh_token"
### Configuration variables in Pangu End.

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
  "INSTALLED_WORKFLOW"
  "INSTALLED_TRDPTY_TRIX_DATA"
  "INSTALLED_BACKFILL_WORKFLOW_TRIGGER"
  "INSTALLED_ADH_CREATIVE_WORKFLOW"
  "INSTALLED_ADH_BRANDING_WORKFLOW"
  "INSTALLED_ADH_AUDIENCE_WORKFLOW"
  "INSTALLED_YOUTUBE_WORKFLOW"
  "INSTALLED_CPP_WORKFLOW"
)

fail() {
    echo "$@"
    exit 1
}

enable_service() {
    gcloud services enable $1 || fail "Unable to enable service $1 for project $project_id, please check if you have the permissions"
    echo "[Success] Enabled service $1"
}

create_bq_dataset() {
  local dataset=$1
  local location=$2
  local datasetMetadata

  datasetMetadata="$(bq --format json --dataset_id "${dataset}" show 2>&1)"
  if [[ "${datasetMetadata}" == *"BigQuery error in show operation"* ]]; then
    bq --location="${location}" mk "${dataset}" || fail "Unable to create dataset $dataset."
    echo "[Success] Created dataset $dataset"
  else
    echo "[Success] dataset $dataset already exists, skip creation"
  fi
}

create_gcs_bucket() {
    local bucket=$1
    local gcs_location=$2

    gsutil ls gs://$bucket >/dev/null 2>&1
    
    if [ $? -ne 0 ] ; then
        gsutil mb -l $gcs_location "gs://$bucket" || fail "Unable to create bucket $bucket."
        echo "[Success] Created bucket $bucket"
    else
        echo "[Success] Bucket $bucket already exists, skip creation"
    fi
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

make_oauth_token() {
  mkdir "keys"
  local oauth=$(cat <<EOF
{
  "client_id": "${CLIENT_ID}",
  "client_secret": "${CLIENT_SECRET}",
  "token": {
    "access_token": "foobar",
    "expires_in": 3599,
    "refresh_token": "${REFRESH_TOKEN}",
    "scope": "${SCOPE}",
    "token_type": "Bearer"
  }
}
EOF
)
  echo "Saving Oauth token $oauth to file keys/oauth2.token.json ..."
  echo $oauth > $SOLUTION_ROOT/keys/oauth2.token.json
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
  gcloud scheduler jobs pause $1 --location ${REGION}
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

  # Filter the required config files based on the selected lego workflows.
  local taskConfigs
  for config in "${!TASK_CONFIGS[@]}"; do
    if ${TASK_CONFIGS["$config"]}; then
      taskConfigs+=("$config")
    fi
  done

  # Create/update workflow task config and cronjob.
  update_workflow_task "${taskConfigs[@]}"

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

  # Create/update 3rd party data task config and cronjob.
  if [[ ${INSTALLED_TRDPTY_TRIX_DATA,,} = "y" ]]; then
    update_workflow_task "./config/task_trdpty.json"
    local message_body='{
      "timezone":"'"${TIMEZONE}"'",
      "partitionDay": "${today}",
      "datasetId": "'${DATASET_ID}'"
    }'
    update_workflow_cronjob "trdpty_load_reports" "0 7-23 * * *" \
      "${message_body}"
    pause_cloud_scheduler ${PROJECT_NAMESPACE}-trdpty_load_reports
  fi

  # Create/update ADH creative task config and cronjob.
  if [[ ${INSTALLED_ADH_CREATIVE_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_adh.json"
    local message_body='{
      "timezone":"'"${TIMEZONE}"'",
      "partitionDay": "${today}",
      "legoDatasetId": "'"${DATASET_ID}"'",
      "adhCustomerId": "'"${ADH_CID}"'"
    }'
    update_workflow_cronjob "adh_lego_start" "0 13 * * 1" "${message_body}"
    pause_cloud_scheduler ${PROJECT_NAMESPACE}-adh_lego_start
  fi

  # Create/update ADH branding task config and cronjob.
  if [[ ${INSTALLED_ADH_BRANDING_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_adh_branding.json"
    local message_body='{
      "timezone":"'"${TIMEZONE}"'",
      "partitionDay": "${today}",
      "adhCustomerId": "'"${ADH_CID}"'"
    }'
    update_workflow_cronjob "adh_branding_start" "0 12 1 1 *" "${message_body}"
    pause_cloud_scheduler ${PROJECT_NAMESPACE}-adh_branding_start
  fi

  # Create/update ADH audience task config and cronjob.
  if [[ ${INSTALLED_ADH_AUDIENCE_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_retail_adh.json"
    local message_body='{
      "timezone":"'"${TIMEZONE}"'",
      "partitionDay": "${today}",
      "legoDatasetId": "'"${DATASET_ID}"'",
      "adhCustomerId": "'"${ADH_CID}"'"
    }'
    update_workflow_cronjob "adh_audience_start" "0 15 * * 1" "${message_body}"
    pause_cloud_scheduler ${PROJECT_NAMESPACE}-adh_audience_start
  fi

  # Create/update LEGO CPP Extension for GrCN market.
  if [[ ${INSTALLED_CPP_WORKFLOW,,} = "y" ]]; then
    update_workflow_task "./config/workflow_cpp.json"
    # Change MCC list string from comma separated into '\n' separated.
    local mccCids
    mccCids="$(printf '%s' "${MCC_CIDS}" | sed -r 's/,/\\\\n/g')"
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
}

printf "\n" | confirm_project
check_permissions
enable_service "googleads.googleapis.com"
create_bq_dataset "${DATASET_ID}"  "${REGION_FOR_DS}"

if [[ ${INSTALLED_ADH_AUDIENCE_WORKFLOW,,} = "y" ]]; then
  enable_service "adsdatahub.googleapis.com"
  create_bq_dataset "${ADH_AUDIENCE_DS_ID}"  "${REGION_FOR_DS}"
  SCOPE="${SCOPE} https://www.googleapis.com/auth/adsdatahub"
fi
if [[ ${INSTALLED_ADH_BRANDING_WORKFLOW,,} = "y" ]]; then
  enable_service "adsdatahub.googleapis.com"
  create_bq_dataset "${ADH_BRANDING_DS_ID}"  "${REGION_FOR_DS}"
  SCOPE="${SCOPE} https://www.googleapis.com/auth/adsdatahub"
fi
if [[ ${INSTALLED_ADH_CREATIVE_WORKFLOW,,} = "y" ]]; then
  enable_service "adsdatahub.googleapis.com"
  create_bq_dataset "${ADH_CREATIVE_DS_ID}"  "${REGION_FOR_DS}"
  SCOPE="${SCOPE} https://www.googleapis.com/auth/adsdatahub"
fi
create_gcs_bucket "${GCS_BUCKET}" "${REGION}"
create_gcs_bucket "${GCS_CONFIG_BUCKET}" "${REGION}"
confirm_firestore native "${REGION}"
printf "\n" | save_config
create_subscriptions
create_sink
deploy_tentacles
make_oauth_token
deploy_sentinel
set_internal_task
copy_to_gcs sql "gs://${GCS_CONFIG_BUCKET}"
copy_to_gcs config "gs://${GCS_CONFIG_BUCKET}"
set_gcs_lifecycle
update_api_config ./config/config_api.json
initialize_workflow
