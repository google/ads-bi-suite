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
  b.customer_descriptive_name Account,
  b.customer_id Customer_ID,
  b.customer_currency_code Currency,
  b.payment_account_id Payment_account_id,
  b.payment_account_name Payment_account_name,
  b.budget_approved Budget_approved,
  b.budget_served Budget_served,
  b.budget_start_time Budget_start_time,
  b.budget_end_time Budget_end_time,
  b.budget_remain Budget_remain,
  b.spending_limit Spending_limit,
  CASE
    WHEN b.budget_start_time IS NOT NULL THEN b.budget_last
    WHEN b.budget_start_time IS NULL THEN 0
    ELSE 0
    END Budget_last_days,
  w.week1_cost AS Week1_cost,
  w.week2_cost AS Week2_cost,
  w.week1_clicks AS Week1_clicks,
  w.week2_clicks AS Week2_clicks,
  w.week1_conversion_value AS Week1_conversion_value,
  w.week2_conversion_value AS Week2_conversion_value,
  w.week1_all_conversion_value AS Week1_all_conversion_value,
  w.week2_all_conversion_value AS Week2_all_conversion_value,
  w.week1_conversions AS Week1_conversions,
  w.week2_conversions AS Week2_conversions,
  w.week1_impressions AS Week1_impressions,
  w.week2_impressions AS Week2_impressions,
  c.segments_date Date,
  sum(c.metrics_clicks) Clicks,
  sum(c.metrics_impressions) Impressions,
  sum(c.metrics_cost) Cost,
  sum(c.metrics_conversions) Conversions,
  sum(c.metrics_all_conversions_value) Conv_value,
  sum(c.metrics_all_conversions) All_conversions,
  sum(c.metrics_all_conversions_value) All_conv_value,
  sum(c.metrics_conversions_by_conversion_date) Conversions_by_conversion_date,
  sum(c.metrics_view_through_conversions) View_through_conversions
FROM `${datasetId}.nonapp_snd_account_budget` b
LEFT JOIN `${datasetId}.nonapp_snd_account_wow` w
  ON b.customer_id = w.customer_id
LEFT JOIN `${datasetId}.base_snd_campaign_performance` c
  ON b.customer_id = c.customer_id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25