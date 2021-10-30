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

WITH
  raw AS (
    SELECT
      segments.date segments_date,
      campaign.id campaign_id,
      customer.id customer_id,
      segments.ad_network_type segments_ad_network_type,
      segments.device segments_device,
      campaign.advertising_channel_type advertising_channel_type,
      campaign.advertising_channel_sub_type advertising_channel_sub_type,
      SUM(metrics.clicks) metrics_clicks,
      SUM(metrics.conversions_value) metrics_conversions_value,
      SUM(metrics.all_conversions_value) metrics_all_conversions_value,
      SUM(metrics.impressions) metrics_impressions,
      ROUND(SUM(metrics.cost_micros) / 1e6, 2) metrics_cost,
      SUM(metrics.conversions) metrics_conversions,
      SUM(metrics.all_conversions) metrics_all_conversions,
      SUM(metrics.conversions_by_conversion_date) metrics_conversions_by_conversion_date,
      SUM(metrics.view_through_conversions) metrics_view_through_conversions,
      SUM(metrics.video_views) metrics_video_views,
      AVG(metrics.content_budget_lost_impression_share) content_budget_lost_impression_share,
      AVG(metrics.content_rank_lost_impression_share) content_rank_lost_impression_share,
      AVG(metrics.search_impression_share) search_impression_share,
      AVG(metrics.search_budget_lost_impression_share) search_budget_lost_impression_share,
      AVG(metrics.search_rank_lost_impression_share) search_rank_lost_impression_share,
      AVG(metrics.content_budget_lost_impression_share) content_budget_lost_impressions_share,
      AVG(metrics.content_rank_lost_impression_share) content_rank_lost_impressions_share,
      AVG(metrics.video_quartile_p100_rate) metrics_video_quartile_p100_rate,
      AVG(metrics.video_quartile_p25_rate) metrics_video_quartile_p25_rate,
      AVG(metrics.video_quartile_p50_rate) metrics_video_quartile_p50_rate,
      AVG(metrics.video_quartile_p75_rate) metrics_video_quartile_p75_rate
    FROM `${datasetId}.report_base_campaign_performance` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          MAX(_partitionTime) partitionTime
        FROM
          `${datasetId}.report_base_campaign_performance`
        GROUP BY
          1,
          2
      ) t
      ON
        t.partitionTime = r._partitionTime
        AND t.campaign_id = r.campaign.id
        AND t.segments_date = r.segments.date
    GROUP BY 1, 2, 3, 4, 5, 6, 7
  )
SELECT
  segments_date,
  campaign_id,
  customer_id,
  segments_ad_networks,
  segments_device,
  raw.segments_ad_network_type segments_ad_network_type,
  advertising_channel_type,
  advertising_channel_sub_type,
  IFNULL(installs, 0) installs,
  IFNULL(in_app_actions, 0) in_app_actions,
  IFNULL(add_to_cart, 0) add_to_cart,
  IFNULL(purchase, 0) purchase,
  IFNULL(lead, 0) lead,
  IFNULL(signup, 0) signup,
  IFNULL(check_out, 0) check_out,
  IFNULL(metrics_clicks, 0) metrics_clicks,
  IFNULL(metrics_impressions, 0) metrics_impressions,
  IFNULL(metrics_conversions_value, 0) metrics_conversions_value,
  IFNULL(metrics_conversions, 0) metrics_conversions,
  IFNULL(metrics_conversions_by_conversion_date, 0) metrics_conversions_by_conversion_date,
  IFNULL(metrics_cost, 0) metrics_cost,
  IFNULL(metrics_view_through_conversions, 0) metrics_view_through_conversions,
  IFNULL(metrics_all_conversions_value, 0) metrics_all_conversions_value,
  IFNULL(metrics_all_conversions, 0) metrics_all_conversions,
  IFNULL(metrics_video_views, 0) metrics_video_views,
  content_budget_lost_impression_share,
  content_rank_lost_impression_share,
  search_impression_share,
  search_budget_lost_impression_share,
  search_rank_lost_impression_share,
  content_budget_lost_impressions_share,
  content_rank_lost_impressions_share,
  metrics_video_quartile_p25_rate,
  metrics_video_quartile_p50_rate,
  metrics_video_quartile_p75_rate,
  metrics_video_quartile_p100_rate
FROM raw
LEFT JOIN
  (
    SELECT
      segments_date,
      campaign_id,
      COUNT(DISTINCT segments_ad_network_type) segments_ad_networks
    FROM raw
    GROUP BY 1, 2
  )
  USING (segments_date, campaign_id)
LEFT JOIN
  (
    SELECT
      campaign.id campaign_id,
      segments.date segments_date,
      segments.ad_network_type segments_ad_network_type,
      segments.device segments_device,
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
        in_app_actions,
      SUM(
        IF(
          segments.conversion_action_category = "ADD_TO_CART",
          metrics.conversions,
          0))
        add_to_cart,
      SUM(
        IF(
          segments.conversion_action_category = "BEGIN_CHECKOUT",
          metrics.conversions,
          0))
        check_out,
      SUM(
        IF(
          segments.conversion_action_category = "PURCHASE",
          metrics.conversions,
          0))
        purchase,
      SUM(
        IF(
          segments.conversion_action_category = "SIGNUP",
          metrics.conversions,
          0))
        signup,
      SUM(
        IF(
          segments.conversion_action_category
            IN ("SUBMIT_LEAD_FORM", 'LEAD', 'IMPORTED_LEAD'),
          metrics.conversions,
          0))
        lead
    FROM `${datasetId}.report_base_campaign_conversion` r
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
    GROUP BY 1, 2, 3, 4
  ) conv
  USING (campaign_id, segments_date, segments_ad_network_type, segments_device)