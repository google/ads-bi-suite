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
  lego.*,
  users,
  day2_retention,
  day7_retention,
  day30_retention,
  day2_revenue,
  day7_revenue,
  day30_revenue
FROM (
  SELECT
    asset_youtube_video_asset_youtube_video_id video_id,
    video_title,
    video_duration_millis,
    asset_id,
    asset_thumbnail,
    campaign_app_campaign_setting_app_id,
    campaign_name,
    customer_id,
    customer_descriptive_name,
    campaign_id,
    campaign_app_campaign_setting_bidding_strategy_goal_type,
    SUM(metrics_impressions) metrics_impressions,
    SUM(metrics_cost) metrics_cost,
    SUM(metrics_conversions) metrics_conversions,
    SUM(metrics_clicks) metrics_clicks,
    SUM(metrics_all_conversions) metrics_all_conversions,
    SUM(metrics_all_conversions_value) metrics_all_conversions_value
  FROM
    `${legoDatasetId}.app_trd_asset_perf_report`
  WHERE
    ad_group_ad_asset_view_field_type = "YOUTUBE_VIDEO"
    AND segments_date BETWEEN DATE_ADD(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL (0-${dateRangeInDays}) day)
    AND PARSE_DATE('%Y%m%d', '${partitionDay}')
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11) lego
INNER JOIN
    `${datasetId}.adh_bi_calc_video_asset_${partitionDay}` adh
USING
  (video_id,
    campaign_id)

