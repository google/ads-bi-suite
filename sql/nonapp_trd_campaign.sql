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

SELECT DISTINCT
  m.customer_id Customer_ID,
  m.currency Currency,
  m.customer_descriptive_name Account,
  c.segments_date Date,
  c.campaign_id Campaign_ID,
  c.segments_ad_networks Ad_networks,
  c.segments_ad_network_type Ad_network_type,
  c.segments_device Device,
  c.advertising_channel_type Campaign_type,
  c.advertising_channel_sub_type Campaign_sub_type,
  c.installs Installs,
  c.in_app_actions In_app_actions,
  c.add_to_cart Add_to_cart,
  c.purchase Purchase,
  c.lead Lead,
  c.signup Signup,
  c.check_out Check_out,
  c.metrics_clicks Clicks,
  c.metrics_impressions Impressions,
  c.metrics_conversions_value Conv_value,
  c.metrics_conversions Conversions,
  c.metrics_conversions_by_conversion_date Conversions_by_conversion_date,
  c.metrics_conversions_value_by_conversion_date Conversions_value_by_conversion_date,
  c.metrics_cost Cost,
  c.metrics_view_through_conversions View_through_conversions,
  c.metrics_video_views Video_views,
  c.content_budget_lost_impression_share Content_budget_lost_impression_share,
  c.content_rank_lost_impression_share Content_rank_lost_impression_share,
  c.search_impression_share Search_impressions_share,
  c.search_budget_lost_impression_share Search_budget_lost_impression_share,
  c.search_rank_lost_impression_share Search_rank_lost_impression_share,
  c.content_budget_lost_impressions_share Content_budget_lost_impressions_share,
  c.content_rank_lost_impressions_share Content_rank_lost_impressions_share,
  c.metrics_video_quartile_p25_rate,
  c.metrics_video_quartile_p50_rate,
  c.metrics_video_quartile_p75_rate,
  c.metrics_video_quartile_p100_rate,
  m.campaign_name Campaign,
  m.campaign_status Campaign_status,
  m.campaign_bidding_strategy_type Bidding_type,
  m.campaign_app_campaign_setting_app_id,
  m.campaign_app_campaign_setting_app_store,
  m.campaign_app_campaign_setting_bidding_strategy_goal_type,
  m.language_name,
  m.language_code,
  m.country_code,
  m.country_name,
  m.campaign_budget_amount Budget,
  m.campaign_target_roas_target_roas tROAS,
  m.campaign_target_cpa_target_cpa tCPA,
  p.budget AS Budget_7d,
  p.spend_7d AS Cost_7d,
  p.budget_utilization AS Budget_utilization,
  w.week1_cost AS Week1_cost,
  w.week2_cost AS Week2_cost,
  w.week1_clicks AS Week1_clicks,
  w.week2_clicks AS Week2_clicks,
  w.week1_impressions AS Week1_impressions,
  w.week2_impressions AS Week2_impressions,
  w.week1_conversion_value AS Week1_conversion_value,
  w.week2_conversion_value AS Week2_conversion_value,
  w.week1_all_conversion_value AS Week1_all_conversion_value,
  w.week2_all_conversion_value AS Week2_all_conversion_value,
  w.week1_conversions AS Week1_conversions,
  w.week2_conversions AS Week2_conversions,
  a.all_ads,
  a.disapproved_ads,
  a.underreview_ads,
  m.campaign_optimization_score Campaign_optimization_score
FROM `${datasetId}.base_snd_campaign_performance` c
LEFT JOIN `${datasetId}.base_snd_campaigns` m
  ON
    c.campaign_id = m.campaign_id
    AND c.segments_date = m.segments_date
LEFT JOIN `${datasetId}.nonapp_snd_campaign_pacing` p
  ON c.campaign_id = p.id
LEFT JOIN `${datasetId}.nonapp_snd_campaign_ads_approval` a
  ON c.campaign_id = a.campaign_id
LEFT JOIN `${datasetId}.nonapp_snd_campaign_wow` w
  ON c.Campaign_id = w.campaign_id