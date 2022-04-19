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
  youtube_video_title,
  adgroup_id,
  adgroup_name,
  adgroup_status,
  segments_ad_network_type,
  youtube_video_asset_youtube_video_id,
  ad_group_ad_asset_view_performance_label,
  image_asset_full_size_width_pixels,
  image_asset_full_size_height_pixels,
  image_asset_full_size_url,
  asset_name,
  asset_id,
  ad_group_ad_asset_view_field_type,
  asset_link,
  asset_thumbnail,
  metrics_clicks,
  metrics_conversions_value,
  metrics_impressions,
  metrics_cost,
  metrics_conversions,
  metrics_all_conversions,
  metrics_all_conversions_value,
  metrics_installs,
  metrics_in_app_actions
FROM `${datasetId}.app_snd_asset_perf_report` asset
INNER JOIN
  `${datasetId}.base_snd_campaigns` camp
  USING (
    campaign_id,
    segments_date)