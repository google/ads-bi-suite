#!/bin/bash
# Global ENVs for PanGu.
export GCP_PROJECT="your_gcp_project_id"
# Pub/Sub topics, etc.
# Default project namespace is SOLUTION_NAME.
# Note: only lowercase letters, numbers and dashes(-) are allowed.
export REGION="us-central1"
export REGION_FOR_DS="us"

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

CLOUDSDK_AUTH_ACCESS_TOKEN=$(gcloud auth application-default print-access-token) bash deploy.sh