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

SELECT DISTINCT
  cur.*,
  pre.adgroup_approval_status l_adgroup_approval_status,
  pre.asset_approval_status l_asset_approval_status,
  pre.adgroup_review_status l_adgroup_review_status,
  pre.asset_review_status l_asset_review_status,
  pre.partition_time l__partitionTime,
FROM
  (
    SELECT
      campaign_id,
      campaign_name,
      campaign_stauts,
      customer_id,
      customer_name,
      ad_group_id,
      ad_group_name,
      ad.ad_group_ad_ad_id,
      type,
      topic,
      adgroup_approval_status,
      adgroup_review_status,
      partition_time,
      asset_approval_status,
      asset_review_status,
      asset_id,
      camp.campaign.app_campaign_setting.app_id campaign_app_campaign_setting_app_id,
      ad_group_ad_asset_view_field_type
    FROM
      (
        SELECT
          campaign.id campaign_id,
          campaign.name campaign_name,
          campaign.status campaign_stauts,
          customer.id customer_id,
          customer.descriptive_name customer_name,
          g.ad_group.name ad_group_name,
          g.ad_group.id ad_group_id,
          g.ad_group_ad.ad.id ad_group_ad_ad_id,
          v.type type,
          v.topic,
          CASE
            WHEN g.ad_group_ad.policy_summary.approval_status IS NULL THEN 'N/A'
            ELSE
              g.ad_group_ad.policy_summary.approval_status
            END
              adgroup_approval_status,
          CASE
            WHEN g.ad_group_ad.policy_summary.review_status IS NULL THEN 'N/A'
            ELSE
              g.ad_group_ad.policy_summary.review_status
            END
              adgroup_review_status,
          g.ad_group_ad.ad.type ad_group_ad_ad_type,
          DATETIME(g._partitionTime) partition_time,
        FROM
          `${datasetId}.report_base_campaign_ads_approval` g
        LEFT JOIN
          UNNEST(g.ad_group_ad.policy_summary.policy_topic_entries) AS v
        WHERE
          DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
          AND g.ad_group_ad.ad.type IN (
            'APP_AD',
            'APP_ENGAGEMENT_AD')
      ) ad
    JOIN
      `${datasetId}.report_base_campaigns` camp
      ON
        camp.campaign.id = ad.campaign_id
    JOIN
      (
        SELECT
          ad_group_ad_asset_view.field_type ad_group_ad_asset_view_field_type,
          CASE
            WHEN s.ad_group_ad_asset_view.policy_summary.approval_status IS NULL THEN 'N/A'
            ELSE
              s.ad_group_ad_asset_view.policy_summary.approval_status
            END
              asset_approval_status,
          CASE
            WHEN s.ad_group_ad_asset_view.policy_summary.review_status IS NULL THEN 'N/A'
            ELSE
              s.ad_group_ad_asset_view.policy_summary.review_status
            END
              asset_review_status,
          s.asset.id asset_id,
          s.ad_group_ad.ad.id ad_group_ad_ad_id,
        FROM
          `${datasetId}.report_app_disapprovals_ad_group_ad_asset_view` s
        WHERE
          DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      ) asset
      USING (ad_group_ad_ad_id)
    WHERE
      DATE(camp._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  ) cur
LEFT JOIN
  (
    SELECT
      ad_group_id,
      ad.ad_group_ad_ad_id,
      asset_id,
      adgroup_approval_status,
      adgroup_review_status,
      asset_approval_status,
      asset_review_status,
      partition_time,
    FROM
      (
        SELECT
          g.ad_group.id ad_group_id,
          g.ad_group_ad.ad.id ad_group_ad_ad_id,
          v.type type,
          v.topic,
          CASE
            WHEN g.ad_group_ad.policy_summary.approval_status IS NULL THEN 'N/A'
            ELSE
              g.ad_group_ad.policy_summary.approval_status
            END
              adgroup_approval_status,
          CASE
            WHEN g.ad_group_ad.policy_summary.review_status IS NULL THEN 'N/A'
            ELSE
              g.ad_group_ad.policy_summary.review_status
            END
              adgroup_review_status,
          DATETIME(g._partitionTime) partition_time,
        FROM
          `${datasetId}.report_base_campaign_ads_approval` g
        LEFT JOIN
          UNNEST(g.ad_group_ad.policy_summary.policy_topic_entries) AS v
        WHERE
          DATE(_partitionTime) = DATE_ADD(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL -1 DAY)
          AND g.ad_group_ad.ad.type IN (
            'APP_AD',
            'APP_ENGAGEMENT_AD')
      ) ad
    JOIN
      (
        SELECT
          CASE
            WHEN s.ad_group_ad_asset_view.policy_summary.approval_status IS NULL THEN 'N/A'
            ELSE
              s.ad_group_ad_asset_view.policy_summary.approval_status
            END
              asset_approval_status,
          CASE
            WHEN s.ad_group_ad_asset_view.policy_summary.review_status IS NULL THEN 'N/A'
            ELSE
              s.ad_group_ad_asset_view.policy_summary.review_status
            END
              asset_review_status,
          s.asset.id asset_id,
          s.ad_group_ad.ad.id ad_group_ad_ad_id,
        FROM
          `${datasetId}.report_app_disapprovals_ad_group_ad_asset_view` s
        WHERE
          DATE(_partitionTime) = DATE_ADD(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL -1 DAY)
      ) asset
      USING (ad_group_ad_ad_id)
  ) pre
  USING (
    ad_group_id,
    ad_group_ad_ad_id,
    asset_id)