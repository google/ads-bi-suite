-- Copyright 2021 Google LLC.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

SELECT
  a.customer.id AS Customer_ID,
  a.campaign.id AS Campaign_ID,
  a.ad_group_ad.ad.id,
  a.ad_group_ad.policy_summary.approval_status AS Approval_status,
  a.ad_group_ad.policy_summary.review_status AS Review_status,
  a.ad_group_ad.ad.type AS Ad_type,
  policy.topic AS policy_topic,
  policy.type AS policy_type,
  a.campaign.name AS Campaign_name,
  a.customer.descriptive_name AS Account,
  a.campaign.status AS Campaign_status,
  a.campaign.advertising_channel_type AS Campaign_type,
  a.campaign.advertising_channel_sub_type AS Campaign_sub_type
FROM
  `${datasetId}.report_base_campaign_ads_approval` a,
  UNNEST(ad_group_ad.policy_summary.policy_topic_entries) AS policy
WHERE date(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')