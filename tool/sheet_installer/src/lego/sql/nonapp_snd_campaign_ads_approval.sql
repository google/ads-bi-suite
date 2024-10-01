-- Copyright 2023 Google LLC.
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
  customer.id AS customer_id,
  campaign.id AS campaign_id,
  campaign.name AS campaign_name,
  COUNT(
    IF(
      ad_group_ad.policy_summary.approval_status LIKE 'DISAPPROVED',
      ad_group_ad.policy_summary.approval_status,
      NULL)) AS disapproved_ads,
  COUNT(
    IF(
      ad_group_ad.policy_summary.review_status LIKE 'REVIEW_IN_PROGRESS',
      ad_group_ad.policy_summary.approval_status,
      NULL)) AS underreview_ads,
  COUNT(ad_group_ad.ad.id) AS all_ads
FROM `${datasetId}.report_base_campaign_ads_approval`
WHERE date(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
GROUP BY customer_id, campaign_id, campaign_name