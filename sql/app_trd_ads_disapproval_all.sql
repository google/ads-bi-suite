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
  asset.ad_group_id,
  asset.asset_approval_status,
  asset.asset_review_status,
  asset.asset_id,
  asset.asset_type,
  ag.campaign_id,
  ag.customer_descriptive_name,
  ag.customer_id,
  ag.campaign_name,
  ag.ad_group_ad_ad_id,
  ag.date,
  ag.adgroup_approval_status,
  ag.adgroup_review_status
FROM
  (
    SELECT
      s.ad_group.id ad_group_id,
      DATE(s._partitionTime) date,
      CASE
        WHEN s.ad_group_ad_asset_view.policy_summary.approval_status IS NULL
          THEN 'N/A'
        ELSE s.ad_group_ad_asset_view.policy_summary.approval_status
        END asset_approval_status,
      s.ad_group_ad_asset_view.policy_summary.review_status asset_review_status,
      s.asset.id asset_id,
      s.ad_group_ad_asset_view.field_type asset_type,
    FROM `${datasetId}.report_app_disapprovals_ad_group_ad_asset_view` s
    WHERE
      DATE(s._partitionTime) > DATE_ADD(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL -30 DAY)
  ) asset
JOIN
  (
    SELECT
      g.customer.id customer_id,
      g.campaign.id campaign_id,
      g.customer.descriptive_name customer_descriptive_name,
      g.campaign.name campaign_name,
      g.ad_group.id ad_group_id,
      g.ad_group_ad.ad.id ad_group_ad_ad_id,
      DATE(g._partitionTime) date,
      CASE
        WHEN g.ad_group_ad.policy_summary.approval_status IS NULL THEN 'N/A'
        ELSE g.ad_group_ad.policy_summary.approval_status
        END adgroup_approval_status,
      g.ad_group_ad.policy_summary.review_status adgroup_review_status,
    FROM `${datasetId}.report_base_campaign_ads_approval` g
    WHERE
      g.ad_group_ad.ad.type IN ('APP_AD', 'APP_ENGAGEMENT_AD')
      AND DATE(g._partitionTime)
        > DATE_ADD(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL -30 DAY)
  ) ag
  ON asset.ad_group_id = ag.ad_group_id AND asset.date = ag.date