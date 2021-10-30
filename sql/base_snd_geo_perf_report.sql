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
  campaign_id,
  customer_id,
  customer_currency_code currency,
  customer_descriptive_name,
  campaign_name,
  campaign_status,
  campaign_advertising_channel_type,
  campaign_advertising_channel_sub_type,
  segments_week,
  geo_target_constant_canonical_name,
  geographic_view_country_criterion_id,
  segments_ad_network_type segments_ad_network_type,
  IFNULL(metrics_clicks, 0) metrics_clicks,
  IFNULL(metrics_conversions_value, 0) metrics_conversions_value,
  IFNULL(metrics_impressions, 0) metrics_impressions,
  IFNULL(metrics_conversions, 0) metrics_conversions,
  IFNULL(metrics_cost, 0) metrics_cost
FROM
  (
    SELECT
      campaign.id campaign_id,
      customer.id customer_id,
      customer.currency_code customer_currency_code,
      customer.descriptive_name customer_descriptive_name,
      campaign.name campaign_name,
      campaign.status campaign_status,
      campaign.advertising_channel_type campaign_advertising_channel_type,
      campaign.advertising_channel_sub_type campaign_advertising_channel_sub_type,
      segments.week segments_week,
      segments.ad_network_type segments_ad_network_type,
      geographic_view.country_criterion_id geographic_view_country_criterion_id,
      SUM(metrics.clicks) metrics_clicks,
      SUM(metrics.conversions_value) metrics_conversions_value,
      SUM(metrics.impressions) metrics_impressions,
      ROUND(SUM(metrics.cost_micros) / 1e6, 2) metrics_cost,
      SUM(metrics.conversions) metrics_conversions
    FROM `${datasetId}.report_base_geographic_view` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.week segments_week,
          MAX(_partitionTime) partitionTime
        FROM
          `${datasetId}.report_base_geographic_view`
        GROUP BY
          1,
          2
      ) t
      ON
        t.partitionTime = r._partitionTime
        AND t.campaign_id = r.campaign.id
        AND t.segments_week = r.segments.week
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
  ) geo
LEFT JOIN
  (
    SELECT DISTINCT
      geo_target_constant.id geographic_view_country_criterion_id,
      geo_target_constant.canonical_name geo_target_constant_canonical_name
    FROM `${datasetId}.report_base_geo_target_constant`
  ) c
  USING (geographic_view_country_criterion_id)