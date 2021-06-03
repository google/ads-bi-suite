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
  cur.*,
  pre.adgroup_approval_status l_adgroup_approval_status,
  pre.asset_approval_status l_asset_approval_status,
  pre.adgroup_review_status l_adgroup_review_status,
  pre.asset_review_status l_asset_review_status,
  pre.partition_time l__partitionTime,
FROM
  (
    SELECT
      customer_id,
      campaign_id,
      customer_descriptive_name,
      campaign_name,
      ad_group_id,
      ad.ad_group_ad_ad_id,
      type,
      topic,
      adgroup_approval_status,
      adgroup_review_status,
      ad_group_name,
      partition_time,
      asset_approval_status,
      asset_review_status,
      ad_group_ad_asset_view_field_type,
      asset_id
    FROM
      (
        SELECT
          g.customer.id customer_id,
          g.customer.descriptive_name customer_descriptive_name,
          g.campaign.id campaign_id,
          g.campaign.name campaign_name,
          g.ad_group.id ad_group_id,
          g.ad_group_ad.ad.id ad_group_ad_ad_id,
          v.type type,
          v.topic,
          CASE
            WHEN g.ad_group_ad.policy_summary.approval_status IS NULL
              THEN 'N/A'
            ELSE g.ad_group_ad.policy_summary.approval_status
            END adgroup_approval_status,
          g.ad_group_ad.policy_summary.review_status adgroup_review_status,
          g.ad_group_ad.ad.type ad_group_ad_ad_type,
          g.ad_group.name ad_group_name,
          DATE(g._partitionTime) AS partition_time
        FROM `${datasetId}.report_base_campaign_ads_approval` g
        LEFT JOIN UNNEST(g.ad_group_ad.policy_summary.policy_topic_entries) AS v
        WHERE DATE(g._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      ) ad
    JOIN
      (
        SELECT
          CASE
            WHEN
              s.ad_group_ad_asset_view.policy_summary.approval_status
              IS NULL
              THEN 'N/A'
            ELSE s.ad_group_ad_asset_view.policy_summary.approval_status
            END asset_approval_status,
          s.ad_group_ad_asset_view.policy_summary.review_status
            asset_review_status,
          s.ad_group_ad_asset_view.field_type
            ad_group_ad_asset_view_field_type,
          s.asset.id asset_id,
          s.ad_group_ad.ad.id ad_group_ad_ad_id,
        FROM `${datasetId}.report_app_disapprovals_ad_group_ad_asset_view` s
        WHERE DATE(s._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      ) asset
      ON asset.ad_group_ad_ad_id = ad.ad_group_ad_ad_id
  ) cur
LEFT JOIN
  (
    SELECT
      customer_id,
      campaign_id,
      ad_group_id,
      ad.ad_group_ad_ad_id,
      type,
      topic,
      adgroup_approval_status,
      adgroup_review_status,
      ad_group_name,
      partition_time,
      asset_approval_status,
      asset_review_status,
      ad_group_ad_asset_view_field_type,
      asset_id,
      asset_type
    FROM
      (
        SELECT
          g.customer.id customer_id,
          g.campaign.id campaign_id,
          g.ad_group.id ad_group_id,
          g.ad_group_ad.ad.id ad_group_ad_ad_id,
          v.type type,
          v.topic,
          CASE
            WHEN g.ad_group_ad.policy_summary.approval_status IS NULL
              THEN 'N/A'
            ELSE g.ad_group_ad.policy_summary.approval_status
            END adgroup_approval_status,
          g.ad_group_ad.policy_summary.review_status adgroup_review_status,
          g.ad_group_ad.ad.type ad_group_ad_ad_type,
          g.ad_group.name ad_group_name,
          DATE(g._partitionTime) partition_time,
        FROM `${datasetId}.report_base_campaign_ads_approval` g
        LEFT JOIN UNNEST(g.ad_group_ad.policy_summary.policy_topic_entries) AS v
        WHERE
          DATE(g._partitionTime)
            = DATE_ADD(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL -1 DAY)
          AND g.ad_group_ad.ad.type IN ('APP_AD', 'APP_ENGAGEMENT_AD')
      ) ad
    JOIN
      (
        SELECT
          CASE
            WHEN
              s.ad_group_ad_asset_view.policy_summary.approval_status
              IS NULL
              THEN 'N/A'
            ELSE s.ad_group_ad_asset_view.policy_summary.approval_status
            END asset_approval_status,
          s.ad_group_ad_asset_view.policy_summary.review_status
            asset_review_status,
          s.ad_group_ad_asset_view.field_type ad_group_ad_asset_view_field_type,
          s.asset.id asset_id,
          s.asset.type asset_type,
          s.ad_group_ad.ad.id ad_group_ad_ad_id,
        FROM `${datasetId}.report_app_disapprovals_ad_group_ad_asset_view` s
        WHERE
          DATE(_partitionTime)
          = DATE_ADD(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL -1 DAY)
      ) asset
      ON asset.ad_group_ad_ad_id = ad.ad_group_ad_ad_id
  ) pre
  ON (
    pre.ad_group_id = cur.ad_group_id
    AND pre.ad_group_ad_ad_id = cur.ad_group_ad_ad_id
    AND pre.asset_id = cur.asset_id);