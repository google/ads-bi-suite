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
  camp.*,
  segments_conversion_action_name,
  event_name,
  include_in_conversion,
  count_type,
  segments_conversion_source,
  segments_conversion_action,
  metrics_conversions_value,
  metrics_conversions,
  metrics_all_conversions_value,
  metrics_all_conversions,
  segments_ad_network_type,
  installs,
  in_app_actions
FROM
  `${datasetId}.app_snd_campaign_conv_report` r
INNER JOIN
  `${datasetId}.base_snd_campaigns` camp
  USING (
    campaign_id,
    segments_date)