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
  campaign_id,
  campaign_name,
  campaign_app_campaign_setting_app_id,
  campaign_app_campaign_setting_bidding_strategy_goal_type,
  customer_id,
  customer_descriptive_name,
  conv.segments_week,
  segments_conversion_action_name,
  conv.geographic_view_country_criterion_id,
  geo_target_constant_canonical_name,
  metrics_conversions_value,
  metrics_conversions,
  metrics_all_conversions_value,
  metrics_all_conversions
FROM
  (
    SELECT
      campaign.id campaign_id,
      segments.week segments_week,
      segments.conversion_action_name segments_conversion_action_name,
      geographic_view.country_criterion_id geographic_view_country_criterion_id,
      SUM(metrics.conversions_value) metrics_conversions_value,
      SUM(metrics.conversions) metrics_conversions,
      SUM(metrics.all_conversions_value) metrics_all_conversions_value,
      SUM(metrics.all_conversions) metrics_all_conversions
    FROM `${datasetId}.report_app_geo_conversion` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.week segments_week,
          MAX(DATE(_partitionTime)) partitionTime
        FROM
          `${datasetId}.report_app_geo_conversion`
        GROUP BY
          1,
          2
      ) t
      ON
        t.partitionTime = DATE(r._partitionTime)
        AND t.campaign_id = r.campaign.id
        AND t.segments_week = r.segments.week
    GROUP BY 1, 2, 3, 4
  ) conv
INNER JOIN
  (
    SELECT DISTINCT
      geo_target_constant.id geographic_view_country_criterion_id,
      geo_target_constant.canonical_name geo_target_constant_canonical_name
    FROM `${datasetId}.report_base_geo_target_constant`
  ) c
  USING (geographic_view_country_criterion_id)
INNER JOIN `${datasetId}.app_snd_campaigns` camp
  USING (campaign_id, segments_week)