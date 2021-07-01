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
  c.customer.id AS Customer_ID,
  c.customer.descriptive_name AS Account,
  c.campaign.id AS Campaign_ID,
  c.campaign.name AS Campaign,
  c.customer.currency_code AS Currency,
  c.campaign.status AS Campaign_status,
  m.campaign.bidding_strategy,
  m.campaign.bidding_strategy_type AS Bidding_type,
  m.campaign.optimization_score AS Campaign_optimization_score,
  m.campaign.target_cpa.target_cpa_micros / 1000000 AS tCPA,
  m.campaign.target_roas.target_roas AS tROAS,
  p.avg_campaign_optimization_score AS Avg_campaign_optimization_score,
  p.budget AS Budget,
  p.spend_7d AS Cost_7d,
  p.budget_utilization AS Budget_utilization,
  c.segments.date AS Date,
  c.segments.ad_network_type AS Ad_network_type,
  c.segments.device AS Device,
  w.week1_cost AS Week1_cost,
  w.week2_cost AS Week2_cost,
  w.cost_wow AS Cost_WOW,
  c.metrics.clicks AS Clicks,
  w.week1_clicks AS Week1_clicks,
  w.week2_clicks AS Week2_clicks,
  w.clicks_wow AS Clicks_WOW,
  w.week1_conversion_value AS Week1_conversion_value,
  w.week2_conversion_value AS Week2_conversion_value,
  w.week1_all_conversion_value AS Week1_all_conversion_value,
  w.week2_all_conversion_value AS Week2_all_conversion_value,
  w.week1_conversions AS Week1_conversions,
  w.week2_conversions AS Week2_conversions,
  conversions_wow AS Conversions_WOW,
  c.campaign.advertising_channel_type AS Campaign_type,
  c.campaign.advertising_channel_sub_type AS Campaign_sub_type,
  a.all_ads,
  a.disapproved_ads,
  a.underreview_ads,
  c.metrics.cost_micros / 1000000 AS Cost,
  c.metrics.impressions AS Impressions,
  c.metrics.conversions AS Conversions,
  c.metrics.all_conversions AS All_conversions,
  c.metrics.conversions_value AS Conv_value,
  c.metrics.all_conversions_value AS All_conv_value,
  c.metrics.video_views AS Video_view,
  conv.Add_to_cart,
  conv.Purchase,
  conv.Lead,
  conv.Signup,
  conv.Check_out
FROM `${datasetId}.report_base_campaign_performance` c
LEFT JOIN `${datasetId}.nonapp_snd_campaign_perf_pacing` p
  ON c.campaign.id = p.id
LEFT JOIN `${datasetId}.nonapp_snd_campaign_ads_approval` a
  ON a.campaign_id = c.campaign.id
LEFT JOIN `${datasetId}.nonapp_snd_campaign_perf_wow` w
  ON w.Campaign_id = c.campaign.id
LEFT JOIN
  (
    SELECT
      campaign.id,
      segments.date segments_date,
      segments.ad_network_type segments_ad_network_type,
      SUM(
        IF(
          segments.conversion_action_category = "ADD_TO_CART",
          metrics.conversions,
          0))
        Add_to_cart,
      SUM(
        IF(
          segments.conversion_action_category = "BEGIN_CHECKOUT",
          metrics.conversions,
          0))
        Check_out,
      SUM(
        IF(
          segments.conversion_action_category = "PURCHASE",
          metrics.conversions,
          0))
        Purchase,
      SUM(
        IF(
          segments.conversion_action_category = "SIGNUP",
          metrics.conversions,
          0))
        Signup,
      SUM(
        IF(
          segments.conversion_action_category
            IN ("SUBMIT_LEAD_FORM", 'LEAD', 'IMPORTED_LEAD'),
          metrics.conversions,
          0))
        Lead,
    FROM `${datasetId}.report_base_campaign_conversion`
    WHERE
      (DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')) AND metrics.conversions > 0
    GROUP BY 1, 2, 3
  ) conv
  ON
    c.campaign.id = conv.id
    AND c.segments.date = conv.segments_date
    AND c.segments.ad_network_type = conv.segments_ad_network_type
LEFT JOIN `${datasetId}.nonapp_snd_campaigns` m
  ON
    c.campaign.id = m.campaign.id
    AND c.segments.date = m.segments_date
WHERE date(c._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')