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

import os
from typing import Optional

import functions_framework
import info
import requests

from flask import render_template
from google.cloud import bigquery, logging


logging_client = logging.Client()
logging_client.setup_logging()

import logging


PROJECT_ID = os.environ.get('GCP_PROJECT', 'lego-chjerry-lab')
REGION = os.environ.get('FUNCTION_REGION', 'us-central1')
NAMESPACE = os.environ.get('NAMESPACE', 'lego')
GOOGLE_ADS_API_VERSION = os.environ.get('GOOGLE_ADS_API_VERSION', '15')

def create_table_from_sql(sql: str, table:str) -> bigquery.table.RowIterator:
  """Loads bigquery result into the given table.

  Args:
      sql: SQL to run in Big Query.
      table: The table to store the result of gien sql.
  """
  client = bigquery.Client()
  job = client.query(sql, job_config=bigquery.QueryJobConfig(
    destination=table,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND))
  logging.info(f'Loading table {table} with sql:\n{sql}')
  return job.result()

def get_oauth_access_token(
    client_id:str, client_secret:str, refresh_token:str) -> Optional[str]:
  """Gets the Oauth Token.

  Args:
      client_id: Oauth client id.
      client_secret: Oauth client secret.
      refresh_token: Oauth refresh token.

  Returns:
      Optional[str]: The new Oauth access token.
  """
  req = requests.post(
    'https://accounts.google.com/o/oauth2/token',
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'refresh_token',
        'refresh_token': refresh_token,
    })
  if req.status_code != requests.codes.ok:
    return None
  return req.json().get('access_token', None)


def validate_googleads_account(
  google_ads_cid: str,
  google_developer_token: str,
  access_token: str
) -> bool:
  """Validates the Oauth token for Google Ads API.

  Args:
      google_ads_cid: Google Ads account id.
      google_developer_token: Google Ads developer token.
      access_token: The new Oauth access token.

  Returns:
      bool: True if the method successes to communication with Google Ads API.
  """
  logging.info(
      'Test Cid %s, with developer token: %s and access token %s',
      google_ads_cid, google_developer_token, access_token)
  url = (
      f'https://googleads.googleapis.com/v{GOOGLE_ADS_API_VERSION}/'
      f'customers/{google_ads_cid}/googleAds:search'
      f'?query=SELECT customer.id FROM customer'
  )
  headers = {
      'Accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': f'Bearer {access_token}',
      'developer-token': google_developer_token
  }
  req = requests.post(url=url, headers=headers)
  if req.status_code == requests.codes.ok:
    return True
  logging.error(
      'Invalid Google Ads account, (return from Ads API: %s)', req.text)
  return False


@functions_framework.http
def health_check(request):
  del request
  lego_info = info.LegoInfo(
    project_id=PROJECT_ID, namespace=NAMESPACE, region=REGION)

  access_token = get_oauth_access_token(
    client_id=lego_info.oauth_key.get('client_id'),
    client_secret=lego_info.oauth_key.get('client_secret'),
    refresh_token=lego_info.oauth_key.get('token').get('refresh_token')
  )
  google_ads_api_is_valid = all(
    validate_googleads_account(
        google_ads_cid,
        lego_info.google_developer_token,
        access_token
  ) for google_ads_cid in lego_info.google_ads_cids)

  # Pause the cron scheduler job if the oauth token is invalid.
  if (
    not google_ads_api_is_valid and
    info.is_enabled_job(lego_info.cron_scheduler_job)
  ):
    logging.warn(
        'Pause the job (%s) due to invalid oauth token',
        lego_info.cron_scheduler_job.name)
    info.flip_lego_main_cron_job(lego_info.cron_scheduler_job)

  sql_query = f'''
    SELECT
      CURRENT_DATETIME() AS audit_datetime,
      {google_ads_api_is_valid} AS is_oauth_valid_for_ads,
      '{','.join(lego_info.google_ads_cids)}' AS customer_ids,
      '{lego_info.google_developer_token}' AS developer_token
  '''
  create_table_from_sql(
      sql_query,
      f'{lego_info.project_id}.{lego_info.bq_dataset_id}.health_check_result')

  return render_template(
      'info.html',
      google_ads_cids=lego_info.google_ads_cids,
      google_developer_token=lego_info.google_developer_token,
      project_id=lego_info.project_id,
      oauth_key=lego_info.oauth_key,
      google_ads_api_is_valid=google_ads_api_is_valid
  )