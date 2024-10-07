<!--
 Copyright 2023 Google LLC

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 -->

# Apps Google Ads reporting Automation a.k.a. “LEGO”

## 1. Disclaimer and Intro

Copyright 2023 Google LLC.
This is not an official Google product. This solution, including any related
sample code or data, is made available on an “as is,” “as available,” and “with
all faults” basis, solely for illustrative purposes, and without warranty or
representation of any kind. This solution is experimental, unsupported and
provided solely for your convenience. Your use of it is subject to your
agreements with Google, as applicable, and may constitute a beta feature as
defined under those agreements. To the extent that you make any data available
to Google in connection with your use of the solution, you represent and warrant
that you have all necessary and appropriate rights, consents and permissions to
permit Google to use and process that data. By using any portion of this
solution, you acknowledge, assume and accept all risks, known and unknown,
associated with its usage, including with respect to your deployment of any
portion of this solution in your systems, or usage in connection with your
business, if at all.

### 1.1 Apps Google Ads reporting Automation a.k.a. “LEGO”

#### 1.1.1 Brief:
LEGO is a data pipeline solution that helps clients fetch Google Ads metrics and stores the data in clients' Google Cloud Big Query tables.

It is a powerful tool that can help businesses track their ad performance and make informed decisions about their campaigns.

The backend of LEGO leverages two Open-source solutions:
- https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/main/marketing-analytics/activation/data-tasks-coordinator
- https://github.com/GoogleCloudPlatform/cloud-for-marketing/tree/main/marketing-analytics/activation/gmp-googleads-connector

#### 1.1.2 Features:
LEGO offers a number of features that make it a valuable tool for businesses, including:

- The ability to daily fetch Google Ads metrics

#### 1.1.3 Benefits:
Lego can help businesses benefit in a number of ways, including:

- Improved ad performance
- Increased ROI
- Better decision-making
- Increased efficiency

## 2. Deployment

We suggest using a new Cloud Project to install this solution. Following
installation process should be run by the **project owner**.

### 2.1 Preparation

Follow preparation doc to create authentication credentials and Google Ads
developer token.

| Product    | Resource                          | Notes                                                                      |
| ---------- | --------------------------------- | -------------------------------------------------------------------------- |
| Google Ads | MCC account ID                    | The top MCC account contains all the Ads accounts for reporting.           |
| Google Ads | Developer token                   | Check the access level. Should be [Standard access].                       |
| Google Ads | User account                      | Grant access to Google Ads accounts through OAuth during the installation. |
| GCP        | Billed GCP project ID             | This GCP shouldn’t engage with any other Google Ads developer token.       |
| GCP        | User account                      | Login in GCP and install the solution, ideally the 'owner'.                |
| GCP        | OAuth client ID and client secret | The OAuth application’s type should be ‘Desktop app’.                      |
| Other      | Google account                    | **Needs to be submitted before installation to be grant access.**          |

[standard access]: https://developers.google.com/adwords/api/docs/access-levels#access_levels

### 2.2. Check out source codes

1. Open the [Cloud Shell](https://cloud.google.com/shell/)
2. Git clone the repo

### 2.3. Run install script

Run the deployment scrip, and follow the prompt to enter your client id,
client secret, Google Ads developer token, and mcc id.

```shell
cd ads-bi-suite; chmod a+x deploy.sh; ./deploy.sh
```

### 2.4. Possible extra tasks during installation

#### 2.4.1. Initialize [Firestore in Native mode](https://cloud.google.com/datastore/docs/firestore-or-datastore#in_native_mode) in Google Console
#### 2.4.2. Prepare the Google Ads API TOKEN - [See Instruction](https://developers.google.com/google-ads/api/docs/first-call/dev-token?hl=en)

## 3 RoadMap
| Timeline | Item                          | Notes                                                            |
| -------- | ----------------------------- | ---------------------------------------------------------------- |
| Q4"24    | Improve the deployment        | The top MCC account contains all the Ads accounts for reporting. |
