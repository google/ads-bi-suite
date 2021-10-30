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
  segments.date AS Date,
  customer.id AS Customer_ID,
  video.id AS video_id,
  video.title AS video_title,
  round(video.duration_millis / 1e3, 2) AS video_duration_millis,
  campaign.advertising_channel_sub_type AS Campaign_type,
  campaign.name AS Campaign,
  campaign.status AS Campaign_status,
  customer.descriptive_name AS Account,
  customer.currency_code AS Currency,
  campaign.id AS Campaign_ID,
  video.channel_id AS channel_id,
  ad_group.id AS ad_group_id,
  ad_group.name AS ad_group_name,
  metrics.all_conversions AS metrics_all_conversions,
  metrics.all_conversions_value AS metrics_all_conversion_value,
  metrics.view_through_conversions AS metrics_view_through_conversions,
  metrics.impressions AS metrics_impressions,
  metrics.clicks AS metrics_clicks,
  metrics.cost_micros / 1000000 AS metrics_cost,
  metrics.conversions AS metrics_conversion,
  metrics.conversions_value AS metrics_conversions_value,
  metrics.video_views AS metrics_video_views,
  metrics.engagements AS metrics_engagemnets,
  metrics.video_quartile_p25_rate metrics_video_quartile_p25_rate,
  metrics.video_quartile_p50_rate metrics_video_quartile_p50_rate,
  metrics.video_quartile_p75_rate metrics_video_quartile_p75_rate,
  metrics.video_quartile_p100_rate metrics_video_quartile_p100_rate
FROM
  `${datasetId}.report_base_videos`
WHERE
  DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day)