// Copyright 2021 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/** @fileoverview The class stands for OEC installation on top of Cyborg. */

/**
 * Checks for the existence of an external table and creates it if it doesn't exist.
 *
 * @param {string} sheetsUrl The URL of the Google Sheet to use as the
 *   data source for the external table. If not provided, the URL of the
 *   active spreadsheet is used.
 * @param {Object!} resource An object containing information about the resource.
 * @return {Object!} The result of the `gcloud.checkOrInitializeExternalTable`
 *   function, which indicates the status of the operation.
 */
const checkAndCreateExternalTable = (sheetsUrl, resource) => {
  return gcloud.checkOrInitializeExternalTable(
    new FxRateSheet(),
    sheetsUrl || SpreadsheetApp.getActiveSpreadsheet().getUrl(),
    resource.attributeValue
  );
};

/**
 * Retrieves information from a scheduled job on Google Cloud Scheduler.
 *
 * This function fetches details of a scheduled job with a name based on
 * the provided namespace and the string "lego_start". It then extracts a
 * specific property from the job's message data.
 *
 * @param {string} propertyName The name of the property to retrieve from the
 *   job's message data.
 * @returns {string} The value of the specified property from the job's
 *   message data, or undefined if the job or property is not found.
 */
function getScheduledJobInfo(propertyName) {
  const properties = getDocumentProperties();
  const { namespace, projectId, locationId } = properties;
  const scheduler = new CloudScheduler(projectId, locationId);
  const job = scheduler.getJob(`${namespace}-lego_start`);
  if (job.name) {
    const message = JSON.parse(
      Utilities.newBlob(
        Utilities.base64Decode(job.pubsubTarget.data)).getDataAsString());
    return message[propertyName];
  }
}

/** Determines a default value for a resource based on a scheduled job.
 *
 * @param {string} value The default value name.
 * @param {Object!} resource An object containing information about the resource.
 * @return {Object!} The result of the `checkParameter`.
 */
function getDefaultValueFromScheduledJob(value, resource) {
  // Determine the value to use, prioritizing the provided 'value'
  const checkedValue =
    value ? value : getScheduledJobInfo(resource.propertyName);
  // Validate the determined value
  const result = checkParameter(checkedValue, resource);

  // If valid, include the value in the result
  if (result.status === RESOURCE_STATUS.OK) {
    result.value = checkedValue;
  }
  return result;
}

/**
 * The OEC config for infrastructure systems, Tentacles and Sentinel.
 */
const ENABLED_TENTACLES_CONNECTOR = ['PB'];

const MANDATORY_SENTINEL_FEATURE = [
  'GoogleAds',
  'ExternalTableOnSheets',
];
const OPTIONAL_SENTINEL_FEATURE = [
  'ADH',
];

/**
 * The OEC cronjob message template for the daily/hourly Sentinel trigger.
 */
const OEC_CRONJOB_MESSAGE = {
  projectId: "#projectId#",
  locationId: "#locationId#",
  timezone: '#timeZone#',
  namespace: '#namespace#',
  datasetId: '#dataset#',
  configDatasetId: '#configDataset#',
  reportBucket: '#reportBucket#',
  configBucket: '#configBucket#',
  partitionDay: '${today}',
  fromDate: '${today_sub_30_hyphenated}',
  developerToken: '#developerToken#',
  mccCids: '#mccCids#',
};

/**
 * The OEC cronjob message template for the ADH trigger.
 */
const OEC_ADH_CRONJOB_MESSAGE = {
  projectId: "#projectId#",
  locationId: "#locationId#",
  timezone: '#timeZone#',
  namespace: '#namespace#',
  datasetId: '#dataset#',
  legoDatasetId: '#dataset#',
  configDatasetId: '#configDataset#',
  reportBucket: '#reportBucket#',
  configBucket: '#configBucket#',
  partitionDay: '${today}',
  fromDate: '${today_sub_30_hyphenated}',
  developerToken: '#developerToken#',
  mccCids: '#mccCids#',
  adhCustomerId: '#adhCid#',
};

/**
 * The OEC cronjob setting for the daily/hourly Sentinel trigger.
 * NOTE: Backfill cronjob can be done through a trigger for OEC daily job.
 */
const SENTINEL_CRON_JOB = [
  {
    enabled: 'TRUE',
    jobId: '#namespace#-lego_start',
    description: 'LEGO daily job',
    schedule: '0 6 * * *',
    taskId: 'lego_start',
    message: OEC_CRONJOB_MESSAGE,
  },
  {
    enabled: 'TRUE',
    jobId: '#namespace#-lego_start_hourly',
    description: 'LEGO hourly job',
    schedule: '0 7-23 * * *',
    taskId: 'lego_start_hourly',
    message: OEC_CRONJOB_MESSAGE,
  },
  // The LEGO cronjob setting for the ADH Sentinel trigger.
  {
    enabled: 'FALSE',
    jobId: '#namespace#-adh_lego_start',
    description: 'LEGO ADH Creative job',
    schedule: '0 13 * * 1',
    taskId: 'adh_lego_start',
    message: OEC_ADH_CRONJOB_MESSAGE,
  },
  {
    enabled: 'FALSE',
    jobId: '#namespace#-adh_audience_start',
    description: 'LEGO ADH Audience job',
    schedule: '0 15 * * 1',
    taskId: 'adh_audience_start',
    message: OEC_ADH_CRONJOB_MESSAGE,
  },
];

/**
 * The OEC config for Cyborg Mojo solution menu.
 */
const OEC_MOJO_CONFIG = {
  sheetName: 'Step1 - Setting OEC Configurations',
  config: [
    // Solution and Google Cloud Project setting
    { template: 'namespace', value: 'lego', },
    { template: 'timeZone', value: 'Asia/Shanghai', },
    { template: 'projectId', },
    {
      template: 'location',
      editType: RESOURCE_EDIT_TYPE.USER_INPUT,
      value: 'Iowa | us-central1',
    },
    {
      template: 'parameter',
      category: 'Config',
      resource: 'MCC CIDs',
      propertyName: 'mccCids',
      optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
      checkFn: getDefaultValueFromScheduledJob,
    },
    {
      template: 'parameter',
      category: 'Config',
      resource: 'Developer Token',
      propertyName: 'developerToken',
      optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
      checkFn: getDefaultValueFromScheduledJob,
    },
    {
      template: 'parameter',
      category: 'Config',
      resource: 'ADH CID',
      propertyName: 'adhCid',
      optionalType: OPTIONAL_TYPE.DEFAULT_UNCHECKED,
    },
    {
      template: 'secretManager',
      category: 'Config',
      value: 'lego_main_legacy_token',
      editType: RESOURCE_EDIT_TYPE.USER_INPUT,
      optionalType: OPTIONAL_TYPE.DEFAULT_CHECKED,
    },
    // TODO: Check if we need to skip permission check for upgrade.
    { template: 'sentinelPermissions', },
    // The GCP APIs enablement.
    { template: 'sentinelApis', },
    {
      template: 'apis',
      value: ['IAM Service Account Credentials API'], // Extra API for exteral table
    },
    {
      template: 'firestore',
    },

    // BigQuery, Storage settings.
    {
      template: 'bigQueryDataset',
      category: 'Solution',
      resource: 'BQ Dataset for Report',
      value: 'ads_reports_data_v4',
      attributes: [
        {
          attributeName: ATTRIBUTE_NAMES.bigquery.partitionExpiration,
          attributeValue: '0'
        }
      ],
    },
    {
      template: 'bigQueryDataset',
      category: 'Solution',
      resource: 'BQ Dataset for Config',
      value: 'ads_report_configs',
      propertyName: 'configDataset',
      attributes: [
        {
          attributeName: ATTRIBUTE_NAMES.bigquery.partitionExpiration,
          attributeValue: '0'
        }
      ],
    },
    {
      template: 'bigQueryDataset',
      category: 'Solution',
      resource: 'BQ Dataset for ADH Creative',
      value: 'adh_apps_data',
      propertyName: 'adhCreativeDatasetId',
      attributes: [
        {
          attributeName: ATTRIBUTE_NAMES.bigquery.partitionExpiration,
          attributeValue: '0'
        }
      ],
    },
    {
      template: 'bigQueryDataset',
      category: 'Solution',
      resource: 'BQ Dataset for ADH Audience',
      value: 'adh_audience',
      propertyName: 'adhAudienceDatasetId',
      attributes: [
        {
          attributeName: ATTRIBUTE_NAMES.bigquery.partitionExpiration,
          attributeValue: '0'
        }
      ],
    },
    {
      template: 'cloudStorage',
      category: 'Solution',
      attributeValue: 0,
      value: '${namespace}-${projectId_normalized}-config',
      propertyName: 'configBucket',
    },
    {
      template: 'cloudStorage',
      category: 'Solution',
      attributeValue: 3,
      value: '${namespace}-${projectId_normalized}',
      propertyName: 'reportBucket'
    },

    // Sentinel settings.
    {
      template: 'tentaclesConnectors',
      category: 'Solution',
      value: tentacles.getConnectorDesc(ENABLED_TENTACLES_CONNECTOR),
      optionalType: OPTIONAL_TYPE.MANDATORY,
    },
    {
      template: 'sentinelFeatures',
      category: 'Solution',
      value: sentinel.getFeatureDesc(MANDATORY_SENTINEL_FEATURE),
      optionalType: OPTIONAL_TYPE.MANDATORY,
    },
    {
      template: 'sentinelFeatures',
      category: 'Solution',
      value: sentinel.getFeatureDesc(OPTIONAL_SENTINEL_FEATURE),
      optionalType: OPTIONAL_TYPE.DEFAULT_UNCHECKED,
    },
    {
      template: 'sentinelVersion',
      category: 'Solution',
      resource: 'Sentinel Version',
    },
    {
      template: 'sentinelCloudFunctions',
      category: 'Solution',
      resource: 'Sentinel Functions'
    },
    {
      template: 'sentinelCloudFunctions',
      category: 'Solution',
      resource: 'Sentinel Functions',
      value: '${namespace}_report'
    },
    { template: 'sentinelLogRouter', category: 'Solution' },
    { template: 'sentinelInternJob', category: 'Solution' },
    { template: 'serviceAccountRole', category: 'Solution' },

    // Tentacles settings.
    {
      template: 'tentaclesVersion',
      category: 'Solution',
      resource: 'Tentacles Version',
    },
    {
      template: 'tentaclesCloudFunctions',
      category: 'Solution',
      resource: 'Tentacles Functions',
      value: [
        '${namespace}_http',
        '${namespace}_tran',
        '${namespace}_api',
      ],
    },
    {
      category: 'External Data',
      template: 'externalTable',
      resource: 'FX Sheet URL (empty for current Sheets)',
      attributeName: 'Need Access',
      attributeValue: 'Editor',
      checkFn: checkAndCreateExternalTable,
      editType: RESOURCE_EDIT_TYPE.DEFAULT_VALUE,
    },
  ],
  oauthScope: [
    OAUTH_API_SCOPE.GAds,
    OAUTH_API_SCOPE.ADH,
    // TODO: Check if we still need YouTube scope or not.
    // OAUTH_API_SCOPE.YouTube,
    OAUTH_API_SCOPE.Sheets,
  ],
  headlineStyle: {
    backgroundColor: '#202124',
    fontColor: 'white',
  },
};

/**
 * The solution menus.
 */
const SOLUTION_MENUS = [
  new MojoSheet(OEC_MOJO_CONFIG),
  new SecretManagerSheet(),
  {
    menuItem: [
      {
        name: 'Step2 - Generate an OAuth Token - for OEC Installation',
        method: 'showOAuthSidebar',
      },
    ],
  },
  new ApiAccessChecker({
    sheetName: 'Step3 - Validate API Access with OAuth Token',
  }),
  new FileToStorage({
    sheetName: 'Step4 - Upload SQL Files',
    files: DEFAULT_SQL_FILES,
    filePath: '#configBucket#/sql/',
  }),
  new SentinelConfig({
    sheetName: 'Step5 - Upload Task Config Files',
    tasks: DEFAULT_TASK_CONFIGS,
  }),
  new SentinelCronJob({
    sheetName: 'Step6 - Set Up Daily/Hourly/ADH Cronjobs',
    jobs: SENTINEL_CRON_JOB
  }),
  EXPLICIT_AUTH_MENUITEM,
];
