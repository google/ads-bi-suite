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
  conv.segments_conversion_action_name,
  event_name,
  include_in_conversion,
  count_type,
  conversion_actions,
  conversion_source,
  conversion_action_resource,
  metrics_conversions_value,
  metrics_conversions,
  metrics_all_conversions_value,
  metrics_all_conversions,
  metrics_impressions,
  metrics_clicks,
  metrics_cost,
  installs,
  in_app_actions
FROM
  (
    SELECT
      campaign.id campaign_id,
      segments.date segments_date,
      segments.conversion_action_name segments_conversion_action_name,
      segments.conversion_action conversion_action_resource,
      segments.conversion_action_category segments_conversion_action_category,
      segments.external_conversion_source conversion_source,
      SUM(metrics.conversions_value) metrics_conversions_value,
      SUM(metrics.conversions) metrics_conversions,
      SUM(metrics.all_conversions_value) metrics_all_conversions_value,
      SUM(metrics.all_conversions) metrics_all_conversions
    FROM
      `${datasetId}.report_base_campaign_conversion` r
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
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6
  ) conv
LEFT JOIN
  (
    SELECT
      campaign_id,
      segments_date,
      COUNT(DISTINCT segments_conversion_action) conversion_actions,
      SUM(installs) installs,
      SUM(in_app_actions) in_app_actions
    FROM
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          segments.conversion_action segments_conversion_action,
          IF(
            segments.conversion_action_category = "DOWNLOAD",
            SUM(metrics.conversions),
            0)
            installs,
          IF(
            segments.conversion_action_category != "DOWNLOAD",
            SUM(metrics.conversions),
            0)
            in_app_actions
        FROM
          `${datasetId}.report_base_campaign_conversion` r
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
        GROUP BY
          campaign.id,
          segments.date,
          segments.conversion_action_category,
          segments.conversion_action
      )
    GROUP BY
      1,
      2
  )
  USING (
    campaign_id,
    segments_date)
LEFT JOIN
  (
    SELECT DISTINCT
      conversion_action.resource_name conversion_action_resource,
      IF(
        conversion_action.third_party_app_analytics_settings.event_name IS NULL,
        conversion_action.firebase_settings.event_name,
        conversion_action.third_party_app_analytics_settings.event_name)
        event_name,
      conversion_action.include_in_conversions_metric include_in_conversion,
      conversion_action.counting_type count_type
    FROM
      `${datasetId}.report_app_conversion_action`
  ) event
  USING (conversion_action_resource)
INNER JOIN
  (
    SELECT
      campaign.id campaign_id,
      segments.date segments_date,
      SUM(metrics.clicks) metrics_clicks,
      SUM(metrics.impressions) metrics_impressions,
      ROUND(SUM(metrics.cost_micros) / 1e6, 2) metrics_cost
    FROM
      `${datasetId}.report_base_campaign_performance` r
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
    GROUP BY
      1,
      2
  ) perf
  USING (
    campaign_id,
    segments_date)
INNER JOIN
  `${datasetId}.app_snd_campaigns` camp
  USING (
    campaign_id,
    segments_date)