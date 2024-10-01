-- Copyright 2023 Google LLC
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

WITH
  base AS (
    SELECT *
    FROM `${datasetId}.report_nonapp_pmax_assetgroup`
    WHERE
      _PARTITIONTIME IN (
        SELECT MAX(_partitionTime) FROM `${datasetId}.report_nonapp_pmax_assetgroup`
      )
  ),
  asset_group_num AS (
    SELECT DISTINCT
      customer.id AS Customer_ID,
      campaign.id AS Campaign_ID,
      COUNT(DISTINCT asset_group.id) AS asset_group_num
    FROM base
    WHERE asset_group.status != 'REMOVED'
    GROUP BY 1, 2
  ),
  ad_strength_num AS (
    SELECT DISTINCT
      customer.id AS Customer_ID,
      campaign.id AS Campaign_ID,
      avg(
        CASE
          WHEN asset_group.ad_strength IN ('GOOD', 'EXCELLENT')
            THEN 0.68
          WHEN asset_group.ad_strength IN ('AVERAGE')
            THEN 0.35
          ELSE 0
          END) AS ad_strength_num
    FROM base
    WHERE asset_group.status != 'REMOVED'
    GROUP BY 1, 2
  ),
  user_video_num AS (
    SELECT DISTINCT
      customer.id AS Customer_ID,
      campaign.id AS Campaign_ID,
      COUNT(DISTINCT asset.id) AS user_video_num
    FROM base
    WHERE
      asset.source = 'ADVERTISER'
      AND asset_group_asset.field_type LIKE '%VIDEO'
      AND asset_group_asset.status = 'ENABLED'
      AND asset_group_asset.policy_summary.approval_status LIKE 'APPROVED%'
      AND asset_group.status != 'REMOVED'
    GROUP BY 1, 2
  )
SELECT DISTINCT
  customer.id AS Customer_ID,
  customer.currency_code AS Currency,
  customer.descriptive_name AS Account,
  campaign.id AS Campaign_ID,
  campaign.name AS Campaign,
  campaign.status AS Campaign_status,
  campaign.advertising_channel_type AS Campaign_type,
  campaign.advertising_channel_sub_type AS Campaign_sub_type,
  asset_group_num,
  ad_strength_num,
  user_video_num,
  p.spend_7d AS Cost_7d
FROM base b
LEFT JOIN asset_group_num ag
  ON
    b.customer.id = ag.Customer_ID
    AND b.campaign.id = ag.Campaign_ID
LEFT JOIN ad_strength_num ast
  ON
    b.customer.id = ast.Customer_ID
    AND b.campaign.id = ast.Campaign_ID
LEFT JOIN user_video_num uv
  ON
    b.customer.id = uv.Customer_ID
    AND b.campaign.id = uv.Campaign_ID
LEFT JOIN `${datasetId}.nonapp_snd_campaign_pacing` p
  ON b.campaign.id = p.id