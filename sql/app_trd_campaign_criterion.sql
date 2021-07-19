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
  geo_target_constant_canonical_name,
  ifnull(conv.installs, 0) installs,
  ifnull(conv.in_app_actions, 0) in_app_actions,
  segments_ad_network_type segments_ad_network_type,
  metrics_clicks,
  metrics_conversions_value,
  metrics_impressions,
  metrics_conversions,
  metrics_cost
FROM
  (
    SELECT
      campaign.id campaign_id,
      segments.week segments_week,
      segments.ad_network_type segments_ad_network_type,
      geographic_view.country_criterion_id geographic_view_country_criterion_id,
      SUM(metrics.clicks) metrics_clicks,
      SUM(metrics.conversions_value) metrics_conversions_value,
      SUM(metrics.impressions) metrics_impressions,
      ROUND(SUM(metrics.cost_micros) / 1e6, 2) metrics_cost,
      SUM(metrics.conversions) metrics_conversions
    FROM
      `${datasetId}.report_base_geographic_view` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.week segments_week,
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
        AND t.segments_week = r.segments.week
    GROUP BY
      1,
      2,
      3,
      4
  ) geo
INNER JOIN
  (
    SELECT DISTINCT
      geo_target_constant.id geographic_view_country_criterion_id,
      geo_target_constant.canonical_name geo_target_constant_canonical_name
    FROM
      `${datasetId}.report_base_geo_target_constant`
  ) c
  USING (geographic_view_country_criterion_id)
LEFT JOIN
  (
    SELECT
      campaign.id campaign_id,
      segments.week segments_week,
      segments.ad_network_type segments_ad_network_type,
      geographic_view.country_criterion_id geographic_view_country_criterion_id,
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
    FROM
      `${datasetId}.report_app_geo_conversion` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.week segments_week,
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
        AND t.segments_week = r.segments.week
    GROUP BY
      1,
      2,
      3,
      4
  ) conv
  USING (
    campaign_id,
    segments_week,
    geographic_view_country_criterion_id,
    segments_ad_network_type)
INNER JOIN
  `${datasetId}.app_snd_campaigns` camp
  USING (
    campaign_id,
    segments_week)
WHERE
  camp.segments_date = camp.segments_week