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

SELECT DISTINCT
  camp.*,
  adgroup_id,
  adgroup_name,
  adg.adgroup_status,
  segments_ad_network_type,
  adg_impressions,
  adg_clicks,
  adg_cost,
  adg_installs,
  adg_in_app_actions,
  adg_conversions_value,
  headline,
  description,
  image,
  video,
  headline_l,
  description_l,
  image_l,
  video_l
FROM `${datasetId}.app_snd_ad_group_perf_report` adg
INNER JOIN
  `${datasetId}.base_snd_campaigns` camp
  USING (
    campaign_id,
    segments_date)