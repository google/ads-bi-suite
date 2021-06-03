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
  Account,
  a.Customer_ID,
  Day,
  Currency,
  Account_optimization_score,
  Budget_approved,
  Budget_served,
  Budget_start_time,
  Budget_end_time,
  Budget_remain,
  CASE
    WHEN Budget_start_time IS NOT NULL THEN Budget_last
    WHEN Budget_start_time IS NULL THEN 0
    ELSE 0
    END Budget_last_days,
  clicks AS Clicks,
  w.clicks_wow AS Clicks_WOW,
  impressions AS Impressions,
  cost AS Cost,
  week1_cost AS Week1_cost,
  week2_cost AS Week2_cost,
  week1_clicks AS Week1_clicks,
  week2_clicks AS Week2_clicks,
  week1_conversions AS Week1_conversions,
  week2_conversions AS Week2_conversions,
  week1_conversion_value AS Week1_conversion_value,
  week2_conversion_value AS Week2_conversion_value,
  week1_all_conversion_value AS Week1_all_conversion_value,
  week2_all_conversion_value AS Week2_all_conversion_value,
  w.cost_wow AS Cost_WOW,
  conversions AS Conversions,
  w.conversions_wow AS Conversions_WOW,
  Conv_value,
  All_conversions,
  All_conv_value
FROM `${datasetId}.nonapp_snd_account_perf_budget` a
LEFT JOIN `${datasetId}.nonapp_snd_account_perf_cost_wow` w
  ON a.Customer_ID = w.Customer_id