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

SELECT
  conv.*,
  event_name,
  include_in_conversion,
  count_type
FROM
  (
    SELECT
      campaign.id campaign_id,
      segments.date segments_date,
      segments.conversion_action_name segments_conversion_action_name,
      segments.conversion_action segments_conversion_action,
      segments.conversion_action_category segments_conversion_action_category,
      segments.external_conversion_source segments_conversion_source,
      segments.ad_network_type segments_ad_network_type,
      IFNULL(SUM(metrics.conversions_value), 0) metrics_conversions_value,
      IFNULL(SUM(metrics.conversions), 0) metrics_conversions,
      IFNULL(SUM(metrics.all_conversions_value), 0) metrics_all_conversions_value,
      IFNULL(SUM(metrics.all_conversions), 0) metrics_all_conversions,
      IFNULL(
        IF(
          segments.conversion_action_category = "DOWNLOAD",
          SUM(metrics.conversions),
          0),
        0)
        installs,
      IFNULL(
        IF(
          segments.conversion_action_category != "DOWNLOAD",
          SUM(metrics.conversions),
          0),
        0)
        in_app_actions
    FROM
      `${datasetId}.report_base_campaign_conversion` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          MAX(_partitionTime) partitionTime
        FROM
          `${datasetId}.report_base_campaign_conversion`
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
      6,
      7
  ) conv
LEFT JOIN
  (
    SELECT DISTINCT
      conversion_action.resource_name segments_conversion_action,
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
  USING (segments_conversion_action)
WHERE
  metrics_conversions > 0