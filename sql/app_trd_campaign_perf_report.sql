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
  camp.*,
  segments_ad_networks,
  network.segments_ad_network_type segments_ad_network_type,
  ifnull(installs, 0) installs,
  ifnull(in_app_actions, 0) in_app_actions,
  metrics_clicks,
  metrics_conversions_value,
  metrics_impressions,
  metrics_conversions,
  metrics_cost
FROM
  (
    SELECT
      segments.date segments_date,
      campaign.id campaign_id,
      segments.ad_network_type segments_ad_network_type,
      SUM(metrics.clicks) metrics_clicks,
      SUM(metrics.conversions_value) metrics_conversions_value,
      SUM(metrics.impressions) metrics_impressions,
      ROUND(SUM(metrics.cost_micros) / 1e6, 2) metrics_cost,
      SUM(metrics.conversions) metrics_conversions
    FROM `${datasetId}.report_base_campaign_performance` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          MAX(DATE(_partitionTime)) partitionTime
        FROM
          `${datasetId}.report_base_campaign_performance`
        GROUP BY
          1,
          2
      ) t
      ON
        t.partitionTime = DATE(r._partitionTime)
        AND t.campaign_id = r.campaign.id
        AND t.segments_date = r.segments.date
    GROUP BY 1, 2, 3
  ) network
LEFT JOIN
  (
    SELECT
      segments.date segments_date,
      campaign.id campaign_id,
      COUNT(DISTINCT segments.ad_network_type) segments_ad_networks
    FROM `${datasetId}.report_base_campaign_performance` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          MAX(DATE(_partitionTime)) partitionTime
        FROM
          `${datasetId}.report_base_campaign_performance`
        GROUP BY
          1,
          2
      ) t
      ON
        t.partitionTime = DATE(r._partitionTime)
        AND t.campaign_id = r.campaign.id
        AND t.segments_date = r.segments.date
    GROUP BY 1, 2
  )
  USING (segments_date, campaign_id)
LEFT JOIN
  (
    SELECT
      campaign.id campaign_id,
      segments.date segments_date,
      segments.ad_network_type segments_ad_network_type,
      SUM(
        IF(
          segments.conversion_action_category = "DOWNLOAD",
          metrics.conversions,
          0))
        installs,
      SUM(
        IF(
          segments.conversion_action_category != "DOWNLOAD",
          metrics.conversions,
          0))
        in_app_actions
    FROM `${datasetId}.report_base_campaign_conversion` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          MAX(DATE(_partitionTime)) partitionTime
        FROM
          `${datasetId}.report_base_campaign_conversion`
        GROUP BY
          1,
          2
      ) t
      ON
        t.partitionTime = DATE(r._partitionTime)
        AND t.campaign_id = r.campaign.id
        AND t.segments_date = r.segments.date
    GROUP BY 1, 2, 3
  ) conv
  USING (campaign_id, segments_date, segments_ad_network_type)
LEFT JOIN `${datasetId}.app_snd_campaigns` camp
  USING (campaign_id, segments_date)