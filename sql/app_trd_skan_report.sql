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
  camp.*,
  skan_cv,
  skan_event_type,
  skan_source_app_id,
  skan_user_type,
  skan_conversions,
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
      campaign.id AS campaign_id,
      campaign.name,
      segments.date AS segments_date,
      segments.sk_ad_network_conversion_value skan_cv,
      segments.sk_ad_network_ad_event_type skan_event_type,
      segments.sk_ad_network_source_app.sk_ad_network_source_app_id skan_source_app_id,
      segments.sk_ad_network_user_type skan_user_type,
      SUM(metrics.sk_ad_network_conversions) skan_conversions
    FROM `${datasetId}.report_app_skan` r
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
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  ) skan
INNER JOIN `${datasetId}.base_snd_campaign_performance` perf
  USING (
    campaign_id,
    segments_date)
INNER JOIN
  `${datasetId}.base_snd_campaigns` camp
  USING (
    campaign_id,
    segments_date)