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
  asset_approval_status,
  asset_review_status,
  asset_id,
  asset.customer_id,
  asset.campaign_status,
  ad_group_ad_asset_view_field_type,
  campaign_app_campaign_setting_app_id,
  ag.ad_group_name,
  ag.campaign_id,
  ag.customer_descriptive_name,
  ag.campaign_name,
  ag.date,
  ag.datetime,
  ag.adgroup_approval_status,
  ag.adgroup_review_status,
  type,
  topic
FROM
  (
    SELECT
      ad_group.id ad_group_id,
      campaign.id campaign_id,
      campaign.name campaign_name,
      ad_group.name ad_group_name,
      customer.descriptive_name customer_descriptive_name,
      v.type,
      v.topic,
      DATE(_partitionTime) date,
      DATETIME(_partitionTime) datetime,
      CASE
        WHEN ad_group_ad.policy_summary.approval_status IS NULL THEN 'N/A'
        ELSE
          ad_group_ad.policy_summary.approval_status
        END
          adgroup_approval_status,
      CASE
        WHEN ad_group_ad.policy_summary.review_status IS NULL THEN 'N/A'
        ELSE
          ad_group_ad.policy_summary.review_status
        END
          adgroup_review_status,
    FROM
      `${datasetId}.report_base_campaign_ads_approval`
    LEFT JOIN
      UNNEST(ad_group_ad.policy_summary.policy_topic_entries) AS v
    WHERE
      campaign.advertising_channel_type = 'MULTI_CHANNEL'
      AND campaign.status = 'ENABLED'
      AND ad_group.status = 'ENABLED'
  ) ag
LEFT JOIN
  (
    SELECT
      asset.id asset_id,
      ad_group.id ad_group_id,
      campaign.app_campaign_setting.app_id campaign_app_campaign_setting_app_id,
      customer.id customer_id,
      campaign.status campaign_status,
      DATE(_partitionTime) date,
      CASE
        WHEN ad_group_ad_asset_view.policy_summary.approval_status IS NULL THEN 'N/A'
        ELSE
          ad_group_ad_asset_view.policy_summary.approval_status
        END
          asset_approval_status,
      CASE
        WHEN ad_group_ad_asset_view.policy_summary.review_status IS NULL THEN 'N/A'
        ELSE
          ad_group_ad_asset_view.policy_summary.review_status
        END
          asset_review_status,
      ad_group_ad_asset_view.field_type ad_group_ad_asset_view_field_type
    FROM
      `${datasetId}.report_app_disapprovals_ad_group_ad_asset_view`
    WHERE
      campaign.advertising_channel_type = 'MULTI_CHANNEL'
      AND campaign.status = 'ENABLED'
  ) asset
  USING (
    ad_group_id,
    date)