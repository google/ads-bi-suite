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
  campaign_id Campaign_ID,
  customer_id Customer_ID,
  currency Currency,
  customer_descriptive_name Account,
  campaign_name Campaign,
  campaign_status Campaign_status,
  campaign_advertising_channel_type AS Campaign_type,
  campaign_advertising_channel_sub_type AS Campaign_sub_type,
  segments_week Week,
  geo_target_constant_canonical_name Country_Territory,
  geographic_view_country_criterion_id,
  segments_ad_network_type Ad_network_type,
  metrics_clicks Clicks,
  metrics_conversions_value Conv_value,
  metrics_impressions Impressions,
  metrics_conversions Conversions,
  metrics_cost Cost
FROM `${datasetId}.base_snd_geo_perf_report`