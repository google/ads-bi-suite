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

WITH
  base AS (
    SELECT
      campaign.id AS campaign_id,
      segments.date AS segments_date,
      segments.sk_ad_network_fine_conversion_value AS skan_cv,
      segments.sk_ad_network_ad_event_type AS skan_event_type,
      segments.sk_ad_network_source_app.sk_ad_network_source_app_id AS skan_source_app_id,
      segments.sk_ad_network_user_type AS skan_user_type,
      SUM(metrics.sk_ad_network_installs) AS skan_conversions
    FROM
      `${datasetId}.report_app_skan` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          MAX(_partitionTime) partitionTime
        FROM
          `${datasetId}.report_app_skan`
        GROUP BY
          1,
          2
      ) t
      ON
        t.partitionTime = r._partitionTime
        AND t.campaign_id = r.campaign.id
        AND t.segments_date = r.segments.date
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
  )
SELECT DISTINCT
  camp.*,
  skan_cv,
  skan_event_type,
  skan_source_app_id,
  skan_user_type,
  skan_conversions,
  skans,
  metrics_cost,
  installs,
  in_app_actions,
  metrics_clicks,
  metrics_impressions,
  metrics_conversions_value,
  metrics_conversions,
  metrics_all_conversions_value,
  metrics_all_conversions
FROM
  (
    SELECT
      base.campaign_id,
      base.segments_date,
      skan_cv,
      skan_event_type,
      skan_source_app_id,
      skan_user_type,
      skan_conversions,
      skans
    FROM
      base
    LEFT JOIN
      (
        SELECT
          campaign_id,
          segments_date,
          COUNT(*) skans
        FROM
          base
        GROUP BY
          1,
          2
      )
      USING (campaign_id, segments_date)
  ) skan
INNER JOIN
  (
    SELECT
      campaign_id,
      segments_date,
      SUM(metrics_cost) metrics_cost,
      SUM(installs) installs,
      SUM(in_app_actions) in_app_actions,
      SUM(metrics_clicks) metrics_clicks,
      SUM(metrics_impressions) metrics_impressions,
      SUM(metrics_conversions_value) metrics_conversions_value,
      SUM(metrics_conversions) metrics_conversions,
      SUM(metrics_all_conversions_value) metrics_all_conversions_value,
      SUM(metrics_all_conversions) metrics_all_conversions
    FROM
      `${datasetId}.base_snd_campaign_performance`
    GROUP BY
      1,
      2
  ) perf
  USING (
    campaign_id,
    segments_date)
INNER JOIN
  `${datasetId}.base_snd_campaigns` camp
  USING (
    campaign_id,
    segments_date)
