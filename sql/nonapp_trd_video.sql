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
  segments.date segments_date,
  customer.id AS customer_id,
  video.id video_id,
  video.title video_title,
  round(video.duration_millis / 1e3, 2) video_duration_millis,
  campaign.advertising_channel_sub_type campaign_advertising_channel_sub_type,
  campaign.name campaign_name,
  campaign.status campaign_status,
  ad_group.id ad_group_id,
  ad_group.name ad_group_name,
  metrics.impressions,
  metrics.clicks metrics_clicks,
  metrics.conversions metrics_conversion,
  metrics.conversions_value metrics_conversions_value,
  metrics.video_views metrics_video_views,
  metrics.engagements metrics_engagemnets,
  metrics.video_quartile_p25_rate metrics_video_quartile_p25_rate,
  metrics.video_quartile_p50_rate metrics_video_quartile_p50_rate,
  metrics.video_quartile_p75_rate metrics_video_quartile_p75_rate,
  metrics.video_quartile_p100_rate metrics_video_quartile_p100_rate
FROM
  `${datasetId}.report_base_videos`
WHERE
  DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day)