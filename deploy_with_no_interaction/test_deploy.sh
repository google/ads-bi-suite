#!/bin/bash
# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Global ENVs for PanGu.
export project_id="your_gcp_project_id"
export GCP_PROJECT="$project_id"
# Pub/Sub topics, etc.
# Default project namespace is SOLUTION_NAME.
# Note: only lowercase letters, numbers and dashes(-) are allowed.
export REGION="us-central1"
export REGION_FOR_DS="us"
export REGION_FOR_GCS="us"

# The main workflow that this instance will install. There are following
# available workflows:
# 1. App
# 2. NonApp
# 3. NonAppLite
# 4. App + NonApp
# 5. App + NonAppLite
export INSTALLED_WORKFLOW="App + NonApp"
# Other functionality, e.g. ADH or Google Sheet, etc.
export INSTALLED_ADH_CREATIVE_WORKFLOW="N"
export INSTALLED_ADH_BRANDING_WORKFLOW="N"
export INSTALLED_ADH_AUDIENCE_WORKFLOW="N"
export INSTALLED_TRDPTY_TRIX_DATA="N"
export INSTALLED_BACKFILL_WORKFLOW_TRIGGER="N"
export INSTALLED_CPP_WORKFLOW="N"

export DEVELOPER_TOKEN="your_ads_developer_token"
export MCC_CIDS="your_ads_cids_separate_with_\\n"
export ADH_CID="your_adh_cid"
export CLIENT_ID="your_oauth_client_id"
export CLIENT_SECRET="your_oauth_client_secret"
export REFRESH_TOKEN="your_oauth_refresh_token"

# To only upload the latest CPP related sql files for GrCN 45 Agencies.
export ONLY_UPDLOAD_CPP_SQL="Y"

CLOUDSDK_AUTH_ACCESS_TOKEN=$(gcloud auth application-default print-access-token) bash deploy.sh