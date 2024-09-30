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

/** @fileoverview The class stands for LEGO installation on top of Cyborg. */

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

/** Determines a default value for a resource based on a scheduled job. */
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
 * The LEGO config for infrastructure systems, Tentacles and Sentinel.
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
 * The LEGO config for Cyborg Mojo solution menu.
 */
const LEGO_MOJO_CONFIG = {
  sheetName: 'LEGO - Setting',
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
  new MojoSheet(LEGO_MOJO_CONFIG),
  new FxRateSheet(),
  new SecretManagerSheet({
    sheetName: 'Auth - Oauth Token Manager',
  }),
  new ApiAccessChecker({
    sheetName: 'Tool - LEGO',
  }),
  OAUTH_MENUITEM,
  EXPLICIT_AUTH_MENUITEM,
];
