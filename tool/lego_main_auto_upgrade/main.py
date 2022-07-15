#!/usr/bin/python
# -*- coding: UTF-8 -*-

"""Fetches the Oauth token key from the exist Lego main Google Cloud Function and re-deploy it.

To re-deploy the upgrade Lego main Google Cloud Function, the program fetches
the Oauth token key from the exist Google Cloud Function by searching the
source code zip file in the bucket of Google Cloud built-in bucket for cloud
function deployment and then unzip the file into a temporary file which stored
the source code of the new Lego main Google Cloud Function and re-deploy it by
gcloud cmd.

  The prerequisites:

    1. Follow https://cloud.google.com/sdk/gcloud/reference/auth/application-default
       to run $ gcloud auth application-default login .
    2. Follow https://cloud.google.com/iam/docs/understanding-roles#prerequisite_for_this_guide
       and make sure the runner has granted the following roles in the give
       Google Cloud Project.

       Required roles:
         1. Storage Admin - roles/storage.admin
         2. Cloud Functions Developer - roles/cloudfunctions.developer
         3. Service Account User - roles/iam.serviceAccountUser

  Typical usage example:

    $ python main.py --gcp lego-apple --gcp lego-chjerry-lab
"""

import os
import re
import shutil
import zipfile

from typing import Union, Sequence, Iterable

from absl import app
from absl import flags
from absl import logging

# Imports the Google Cloud client library
from google.cloud import storage

_GCP_PROJECT_IDS = flags.DEFINE_multi_string(
    'gcp_project_id', ['lego-chjerry-lab'],
    'The Google Cloud Project id.', short_name='gcp')

_LEGO_MAIN_INDEX_FILE_PATH = flags.DEFINE_string(
    'lego_main_index_file_path',
    '../../index.js',
    'The filepath to index.js for Lego main Google Cloud Function.')

_LEGO_MAIN_PACKAGE_FILE_PATH = flags.DEFINE_string(
    'lego_main_package_file_path',
    '../../package.json',
    'The filepath to package.json for Lego main Google Cloud Function.')

_OAUTH_TOKEN_FILE_PATH = flags.DEFINE_string(
    'oauth_token_file_path',
    'keys/oauth2.token.json',
    'The filepath to oauth token key in the zip file of Lego main Google Cloud Function.')

_DRY_RUN = flags.DEFINE_bool(
    'dry_run',
    True,
    'Dry run mode only fetches Oauth token key without deployment.',
    short_name='d')

def _deploy_lego_main(
    gcp_project_id: str,
    lego_name_space: str,
    region: str,
    source_path: str
) -> None:
  """Deploys Lego Main Google Cloud Function.

  Args:
      gcp_project_id (str): The Google Cloud Project id.
      lego_name_space (str): The Lego solution namespace.
      region (str): The region code in Google Cloud.
      source_path (str): The tmp folder name to store files for
        Lego main Google Cloud Function.
  """
  logging.info(
      'Deploy Lego Main (gcp id: %s, name space: %s, region: %s, source path: %s)',
      gcp_project_id, lego_name_space, region, source_path)

  os.system((
      f'gcloud functions deploy {lego_name_space}_main '
      '--entry-point=coordinateTask '
      f'--project={gcp_project_id} '
      f'--trigger-topic={lego_name_space}-monitor '
      f'--region={region} '
      f'--source=./{source_path} '
      f'--no-allow-unauthenticated '
      f'--timeout=540 --memory=2048MB --runtime=nodejs14 '
      f'--set-env-vars=OAUTH2_TOKEN_JSON=./keys/oauth2.token.json '
      f'--set-env-vars=GCP_PROJECT={gcp_project_id} '
      f'--set-env-vars=PROJECT_NAMESPACE={lego_name_space} '
      f'--set-env-vars=DEBUG=false '
      f'--set-env-vars=IN_GCP=true'))
  return


def _is_gcf_bucket(name: str) -> bool:
  """Checks if the bucket of Google Cloud built-in bucket for Google Cloud Function deployment.

  If the bucket is Google Cloud built-in bucket for Google Cloud Function
  deployment, the name of the bucket will start with gcf-sources.

  Args:
      name (str): The name of Google Cloud Storage bucket.

  Returns:
      bool: True if the name of bucket is matched.
  """
  return name.startswith('gcf-sources-')


def _is_lego_main_zip_blob(name: str) -> bool:
  """Checks if the blob is the zip file of Lego main Google Cloud Function.

  Args:
      name (str): The name of Google Cloud Storage blob.

  Returns:
      bool: True if the name of blob is matched.
  """
  return re.search('.*_main-.*.zip', name) is not None


def _get_gcf_region_from_bucket_name(name: str) -> str:
  """Parses the Google Cloud region from the name of Google Cloud built-in bucket.

  The name of Google Cloud built-in bucket looks like
  'gcf-sources-642343838549-us-central1', which format is
  gcf-sources + Google Cloud project id + the region code in Google Cloud.

  Args:
      name (str): The name of Google Cloud Storage bucket.

  Returns:
      str: The region code in Google Cloud.
  """
  return '-'.join(name.split('-')[3:])


def _get_lego_name_space_from_blob_name(name: str) -> str:
  """Parses the Lego solution namespace from the name of the given blob.

  Args:
      name (str): The name of Google Cloud Storage blob of
        Lego main Google Cloud Function.

  Returns:
      str: The Lego solution namespace.
  """
  return name.split('_main-')[0]


def _list_lego_main_buckets(
    storage_client: storage.Client
) -> Iterable[storage.bucket.Bucket]:
  """Returns the Google Cloud built-in buckets for Google Cloud Function deployment.

  Args:
      storage_client (storage.Client): The client lib for Google Cloud Storeage.

  Returns:
      Iterable[storage.bucket.Bucket]: The iterator of matched Google Cloud
        Storeage buckets.
  """
  return filter(lambda b: _is_gcf_bucket(b.name), storage_client.list_buckets())


def _get_latest_lego_main_blob(
    storage_client: storage.Client,
    bucket: storage.bucket.Bucket
) -> Union[storage.blob.Blob, None]:
  """Gets the blob

  Args:
      storage_client (storage.Client): The client lib for Google Cloud Storeage.
      bucket (storage.bucket.Bucket): The Google Cloud built-in buckets for
        Google Cloud Function deployment.

  Returns:
      Union[storage.blob.Blob, None]: The latest blob of zip file of
        Lego main Google Cloud Function.
  """
  b = None
  for blob in storage_client.list_blobs(bucket):
    if _is_lego_main_zip_blob(blob.name):
      if not b or b.time_created <= blob.time_created:
        b = blob
  return b


def _prep_lego_files(
    lego_source_code_temp_folder: str,
    lego_main_index_file_path: str,
    lego_main_package_file_path: str
) -> None:
  """Prepares the lego files for Lego main Google Cloud Function further deployment.

  Args:
      lego_source_code_temp_folder (str): The tmp folder name to store files
        for Lego main Google Cloud Function.
      lego_main_index_file_path (str): The filepath to index.js for
        Lego main Google Cloud Function.
      lego_main_package_file_path (str): The filepath to package.json for
        Lego main Google Cloud Function.
  """
  logging.info(
      'Prepare the files from %s to %s',
      lego_main_index_file_path,
      lego_source_code_temp_folder)
  os.makedirs(
      f'{lego_source_code_temp_folder}/keys', 0o777, exist_ok=True)
  shutil.copyfile(
      lego_main_index_file_path, f'{lego_source_code_temp_folder}/index.js')
  shutil.copyfile(
      lego_main_package_file_path,
      f'{lego_source_code_temp_folder}/package.json')


def _clean_up_lego_files(
  lego_main_zip_file_name: str,
  lego_source_code_temp_folder: str
) -> None:
  """Cleans up the lego files after the Lego main Google Cloud Function deployment.

  Args:
      lego_main_zip_file_name (str): The zip file name in
        Lego main Google Cloud Function.
      lego_source_code_temp_folder (str): The tmp folder name to store files
        for Lego main Google Cloud Function.
  """
  logging.info(
      'Remove the files in %s and %s',
      lego_main_zip_file_name,
      lego_source_code_temp_folder)
  if os.path.exists(
      f'{os.getcwd()}/{lego_main_zip_file_name}'
  ):
    os.remove(lego_main_zip_file_name)
  shutil.rmtree(lego_source_code_temp_folder)


def _run(
    gcp_project_id: str,
    storage_client: storage.Client,
    lego_main_zip_file_name: str,
    lego_source_code_temp_folder: str,
    lego_main_index_file_path: str,
    lego_main_package_file_path: str,
    oauth_token_file_path: str,
    dry_run: bool
) -> None:
  """The major method to control the deployment process.

  Currently, the process can break down to 5 steps.
    Step 1: Prepares the lego files for Lego main Google Cloud Function
      further deployment.
    Step 2: Fetch the zip file which contains the source code of running
      Lego main Google Cloud Function and extract the oauth token key from it.
    Step 3: Deploy the files that prepared at Step 1 with oauth token got at
      Step 2 to Lego main Google Cloud Function.
    Step 4: Cleans up the lego files after the Lego main Google Cloud
      Function deployment.

  Args:
      gcp_project_id (str): The Google Cloud Project id.
      storage_client (storage.Client): The client lib for Google Cloud Storeage.
      lego_main_zip_file_name (str): The zip file name in
        Lego main Google Cloud Function.
      lego_source_code_temp_folder (str): The tmp folder name to store
        files for Lego main Google Cloud Function.
      lego_main_index_file_path (str): The filepath to index.js for
        Lego main Google Cloud Function.
      lego_main_package_file_path (str): The filepath to package.json for
        Lego main Google Cloud Function.
      oauth_token_file_path (str): The filepath to oauth token key in the zip
        file of Lego main Google Cloud Function.
      dry_run (bool): If True, program only fetches Oauth token key without
        deployment.
  """

  # Prepares the lego files.
  _prep_lego_files(
      lego_source_code_temp_folder=lego_source_code_temp_folder,
      lego_main_index_file_path=lego_main_index_file_path,
      lego_main_package_file_path=lego_main_package_file_path
  )

  for bucket in _list_lego_main_buckets(storage_client):
    region = _get_gcf_region_from_bucket_name(bucket.name)
    logging.info(
        'The target region for deployment is: %s, (reason: the bucket name is %s)',
        region, bucket.name)
    lego_main_blob = _get_latest_lego_main_blob(storage_client, bucket)
    if not lego_main_blob:
      logging.info(
        'Not found any target in region for deployment. (reason: the bucket name is %s)',
        region, bucket.name)
      return

    lego_name_space = _get_lego_name_space_from_blob_name(lego_main_blob.name)
    logging.info(
        'The LEGO name space is: %s, (reason: bucket name is %s, blob name is %s)',
        lego_name_space, bucket.name, lego_main_blob.name)

    logging.info(
      'Download the source code zip file from %s/%s to %s',
      bucket.name, lego_main_blob.name, lego_main_zip_file_name)

    # Writes the original source code into the given zip file path.
    with open(lego_main_zip_file_name, 'wb') as binary_file:
      binary_file.write(lego_main_blob.download_as_bytes())
      binary_file.close()

    logging.info(
      'Extract the oauth token key from %s to %s/',
      oauth_token_file_path, lego_source_code_temp_folder)

    # Unzips the downloaded zip file path and extracts the Oauth token key.
    with zipfile.ZipFile(lego_main_zip_file_name, 'r') as zip_ref:
      zip_ref.extract(
          oauth_token_file_path, f'{lego_source_code_temp_folder}/')
      zip_ref.close()

    if not os.path.exists(
      f'{os.getcwd()}/{lego_source_code_temp_folder}/{oauth_token_file_path}'
    ):
      logging.error(
          'Give up the deployment, (reason: No Oauth token in %s)',
          oauth_token_file_path)

    if not dry_run:
      logging.info('Skip the step of new deployment in dry run mode.')
    else:
      # Deploys the new Lego main Google Cloud Function with the original
      # Oauth token key.
      _deploy_lego_main(
          gcp_project_id,
          lego_name_space,
          region,
          lego_source_code_temp_folder)

  # Cleans up the downloaded files.
  _clean_up_lego_files(
      lego_main_zip_file_name=lego_main_zip_file_name,
      lego_source_code_temp_folder=lego_source_code_temp_folder
  )

  return


def main(unused_argv: Sequence[str]) -> None:
  del unused_argv  # Unused

  oauth_token_file_path = _OAUTH_TOKEN_FILE_PATH.value
  lego_main_zip_file_name = 'lego_main.zip'
  lego_source_code_temp_folder = 'lego_temp'
  lego_main_index_file_path = _LEGO_MAIN_INDEX_FILE_PATH.value
  lego_main_package_file_path = _LEGO_MAIN_PACKAGE_FILE_PATH.value
  dry_run = _DRY_RUN.value

  for gcp_project_id in _GCP_PROJECT_IDS.value:
    logging.info(
        'Program starts to upgrade Lego Main Google Cloud Functions in %s. Dry run mode: %s ',
        gcp_project_id, not dry_run)

    storage_client = storage.Client(project=gcp_project_id)
    _run(
        gcp_project_id=gcp_project_id,
        storage_client=storage_client,
        lego_main_zip_file_name=lego_main_zip_file_name,
        lego_source_code_temp_folder=lego_source_code_temp_folder,
        lego_main_index_file_path=lego_main_index_file_path,
        lego_main_package_file_path=lego_main_package_file_path,
        oauth_token_file_path=oauth_token_file_path,
        dry_run=dry_run
    )

if __name__ == '__main__':
  flags.mark_flags_as_required(['gcp_project_id'])
  app.run(main)
