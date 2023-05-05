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

#######################################
# Deploy Cloud Functions 'Lego Health Checker'.
# Globals:
#   GCP_PROJECT
#   REGION
#   PROJECT_NAMESPACE
# Arguments:
#   None
#######################################
deploy_cloud_functions_lego_health_checker(){
  local gcp_project region project_namespace
  gcp_project="${1:-"GCP_PROJECT"}"
  region="${2:-"REGION"}"
  project_namespace="${3:-"PROJECT_NAMESPACE"}"
  sourceupload_url="${4:-"SOURCEUPLOAD_URL"}"

  local cf_flag=()
  cf_flag+=(--source=src)
  cf_flag+=(--entry-point=health_check)
  cf_flag+=(--project="${gcp_project}")
  cf_flag+=(--region="${region}")
  # cf_flag+=(--no-allow-unauthenticated)
  cf_flag+=(--allow-unauthenticated --trigger-http)
  cf_flag+=(--timeout=540 --memory=2048MB --runtime=python39)
  cf_flag+=(--set-env-vars=GCP_PROJECT="${gcp_project}")
  cf_flag+=(--set-env-vars=PROJECT_NAMESPACE="${project_namespace}")
  cf_flag+=(--set-env-vars=GOOGLE_ADS_API_VERSION="13")
  cf_flag+=(--set-env-vars=FUNCTION_REGION="${region}")
  cf_flag+=(--set-env-vars=SOURCEUPLOAD_URL="${sourceupload_url}")
  gcloud functions deploy "${project_namespace}"_health_chekcer "${cf_flag[@]}"
  # gcloud alpha functions add-iam-policy-binding lego_health_chekcer \
  #   --project=${gcp_project}
  #   --region=us-central1 \
  #   --member=allUsers \
  #   --role=roles/cloudfunctions.invoker
}

#######################################
# Create/Update Lego Health Checker Scheduler.
# Globals:
#   GCP_PROJECT
#   REGION
#   PROJECT_NAMESPACE
# Arguments:
#   None
#######################################
create_lego_health_checker_job(){
  local gcp_project region project_namespace
  gcp_project="${1:-"GCP_PROJECT"}"
  region="${2:-"REGION"}"
  project_namespace="${3:-"PROJECT_NAMESPACE"}"

  # Create or update a scheduled job to trigger the reader function.
  exist_job=($(gcloud scheduler jobs list --project ${gcp_project} \
    --location ${region} \
    --filter="name~${project_namespace}-${region}-daily-health-check-job" \
    --format="value(state)"))

  if [[ ${#exist_job[@]} -gt 0 ]]; then
    gcloud scheduler jobs update http ${project_namespace}-${region}-daily-health-check-job \
      --description "The daily cronjob to trigger the lego health check" \
      --project=${gcp_project} \
      --location ${region} \
      --schedule "0 0 * * *" \
      --time-zone "Asia/Taipei" \
      --uri "https://${region}-${gcp_project}.cloudfunctions.net/lego_health_chekcer" \
      --http-method POST \
      --oidc-service-account-email ${gcp_project}@appspot.gserviceaccount.com \
      --message-body "{\"foo\": \"bar\"}" || echo "Scheduler already exists"
  else
    gcloud scheduler jobs create http ${project_namespace}-${region}-daily-health-check-job \
      --description "The daily cronjob to trigger the lego health check" \
      --project=${gcp_project} \
      --location ${region} \
      --schedule "0 0 * * *" \
      --time-zone "Asia/Taipei" \
      --uri "https://${region}-${gcp_project}.cloudfunctions.net/lego_health_chekcer" \
      --http-method POST \
      --oidc-service-account-email ${gcp_project}@appspot.gserviceaccount.com \
      --message-body "{\"foo\": \"bar\"}" || echo "Scheduler already exists"
  fi

  gcloud projects add-iam-policy-binding ${gcp_project} \
    --member serviceAccount:${gcp_project}@appspot.gserviceaccount.com \
    --role roles/cloudfunctions.invoker
}

deploy() {
  gcp_project="${1:-"GCP_PROJECT"}"
  exist_jobs=($(gcloud functions list --project=${gcp_project} \
    --filter="name~_main" \
    --format="value[separator=','](environmentVariables.GCP_PROJECT,environmentVariables.PROJECT_NAMESPACE,sourceUploadUrl)"))
  exist_jobs=(${exist_jobs//\\n/})
  for exist_job in "${exist_jobs[@]}"
  do
    IFS=',' read -r -a info <<< "$exist_job"
    projectId="${info[0]}"
    namespace="${info[1]}"
    sourceUploadUrl="${info[2]}"
    region="$(echo ${sourceUploadUrl} | sed s'/\./\t/g' | awk '{print $4}')"
    printf '%s %s %s %s\n' $projectId $namespace $region $sourceUploadUrl
    deploy_cloud_functions_lego_health_checker $projectId $region $namespace
    create_lego_health_checker_job $projectId $region $namespace
  done
}

if [ "$#" -eq 0 ]; then
  GcpIds=()
else
  GcpIds=( "$@" )
fi

for gcp in "${GcpIds[@]}"
do
  deploy $gcp
done
