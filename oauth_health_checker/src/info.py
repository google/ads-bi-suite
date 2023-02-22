import json
import os
import re
import zipfile
import logging
import tempfile

from typing import Iterable, Optional

from google.cloud import scheduler, scheduler_v1, storage


def is_pause_job(job: scheduler_v1.types.job.Job):
  """Returns True if the job is pause.

  Args:
      job: The Cron Scheduler job.

  Returns: True if the job is pause.
  """
  return job.state == scheduler_v1.types.job.Job.State.PAUSED

def is_enabled_job(job: scheduler_v1.types.job.Job):
  """Returns True if the job is enabled.

  Args:
      job: The Cron Scheduler job.

  Returns: True if the job is enabled.
  """
  return job.state == scheduler_v1.types.job.Job.State.ENABLED

def flip_lego_main_cron_job(job: scheduler_v1.types.job.Job):
  """Flips the state of the LEGO main Cron Scheduler.

  Args:
      job: The Cron Scheduler job.

  Returns: True if method successes to flip the state.
  """
  action = None
  scheduler_client = scheduler.CloudSchedulerClient()
  if is_pause_job(job):
    action = scheduler_client.resume_job
  elif is_enabled_job(job):
    action = scheduler_client.pause_job

  action(request={'name': job.name})
  return True

class LegoInfo():
  def __init__(self, project_id:str, namespace: str, region: str):
    """LegoInfo class fetches the Oauth token from the given LEGO solution.

    Args:
        project_id: The GCP project id.
        namespace: The LEGO solution namespace.
        region: The code of region.

    Raises:
        Exception: If no related Cron Scheduler found.
    """
    self.project_id = project_id
    self.namespace = namespace
    self.region = region

    # Inits the GCP service clients.
    self.scheduler_client = scheduler.CloudSchedulerClient()
    self.storage_client = storage.Client()

    self.cron_scheduler_job = self._find_cron_scheduler_job()
    # Raises the error if no cron scheduler found.
    if not self.cron_scheduler_job:
      err_msg = (
          f'No Cron Scheduler found, reason ('
          f'{project_id}:{region}:{namespace})')
      logging.error(err_msg)
      raise Exception(err_msg)

    self.google_ads_cids = self._get_cron_scheduler_job_data(
        'mccCids').split('\\n')
    self.google_developer_token = self._get_cron_scheduler_job_data(
        'developerToken')
    self.bq_dataset_id = self._get_cron_scheduler_job_data(
        'datasetId')
    self.tmpdir = tempfile.gettempdir()
    self._oauth_token_file_path = 'keys/oauth2.token.json'
    self._download_oauth_key()
    self.oauth_key = json.load(open(
        f'{self.tmpdir}/{self._oauth_token_file_path}'))

  def _find_cron_scheduler_job(self) -> Optional[scheduler_v1.types.job.Job]:
    """Finds the related cron scheduler job.

    Returns:
        Optional[scheduler_v1.types.job.Job]: The job.
    """
    jobs = self.scheduler_client.list_jobs(request={
        'parent': f'projects/{self.project_id}/locations/{self.region}'
    })
    target_cf_name = f'{self.namespace}-lego_start'
    for job in jobs:
      if job.name.endswith(target_cf_name):
        return job
    return None

  def _list_lego_main_buckets(self) -> Iterable[storage.bucket.Bucket]:
    """Returns the Google Cloud built-in buckets for
    Google Cloud Function deployment.

    Returns:
        Iterable[storage.bucket.Bucket]: The iterator of matched Google Cloud
          Storeage buckets.
    """

    def _is_gcf_bucket(name: str, region:str) -> bool:
      """Checks if the bucket of Google Cloud built-in bucket
      for Google Cloud Function deployment.

      If the bucket is Google Cloud built-in bucket for Google Cloud Function
      deployment, the name of the bucket will start with gcf-sources.

      Args:
          name: The name of Google Cloud Storage bucket.
          region: The region code in Google Cloud.

      Returns:
          bool: True if the name of bucket is matched.
      """
      bucket_region = '-'.join(name.split('-')[3:])
      return name.startswith('gcf-sources-') and bucket_region == region

    return filter(
        lambda b: _is_gcf_bucket(b.name, self.region),
        self.storage_client.list_buckets())

  def _get_latest_lego_main_blob(
      self,
      bucket: storage.bucket.Bucket
  ) -> Optional[storage.blob.Blob]:
    """Gets the blob.

    Args:
        storage_client (storage.Client): The client lib for Google
          Cloud Storeage.
        bucket (storage.bucket.Bucket): The Google Cloud built-in buckets for
          Google Cloud Function deployment.

    Returns:
        Optional[storage.blob.Blob]: The latest blob of zip file of
          Lego main Google Cloud Function.
    """
    def _is_lego_main_zip_blob(namespace: str, name: str) -> bool:
      """Checks if the blob is the zip file of Lego main Google Cloud Function.

      Args:
          namespace: The LEGO solution namespace.
          name: The name of Google Cloud Storage blob.

      Returns:
          bool: True if the name of blob is matched.
      """
      return re.search(f'.*{namespace}_main-.*.zip', name) is not None
    b = None
    for blob in self.storage_client.list_blobs(bucket):
      if _is_lego_main_zip_blob(self.namespace, blob.name):
        if not b or b.time_created <= blob.time_created:
          b = blob
    return b

  def _get_cron_scheduler_job_data(self, key: str) -> str:
    """Gets the valus by the given name of the key from the Cron Scheduler job.

    Args:
        key: The name of the key to fetch value from the Cron Scheduler job.

    Returns:
        The value of the given key in cron scheduler job.
    """
    data = json.loads(self.cron_scheduler_job.pubsub_target.data)
    return data.get(key, '')

  def _download_oauth_key(self) -> bool:
    """Downloads the oauth key from LEGO main Cloud Function.

    Returns:
        True if succeesses to download the Oauth key.
    """
    lego_main_zip_file_name = f'{self.tmpdir}/lego_main.zip'

    for bucket in self._list_lego_main_buckets():
      logging.info(
          'The target region for deployment is: %s, (reason: the bucket name is %s)',
          self.region, bucket.name)
      lego_main_blob = self._get_latest_lego_main_blob(bucket)
      if not lego_main_blob:
        logging.info(
            'Not found any target in region for deployment. (reason: the bucket name is %s)',
            self.region, bucket.name)
        return False
      logging.info(
          'Download the source code zip file from %s/%s to %s',
          bucket.name, lego_main_blob.name, lego_main_zip_file_name)

      # Writes the original source code into the given zip file path.
      with open(lego_main_zip_file_name, 'wb') as binary_file:
        binary_file.write(lego_main_blob.download_as_bytes())
        binary_file.close()

      logging.info(
          'Extract the oauth token key from %s', self._oauth_token_file_path)

      # Unzips the downloaded zip file path and extracts the Oauth token key.
      with zipfile.ZipFile(lego_main_zip_file_name, 'r') as zip_ref:
        zip_ref.extract(self._oauth_token_file_path, f'{self.tmpdir}/')
        zip_ref.close()

      if not os.path.exists(f'{self.tmpdir}/{self._oauth_token_file_path}'):
        logging.error(
            'Give up, (reason: No Oauth token in %s)', self._oauth_token_file_path)
        return False
    return True
