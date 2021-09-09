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
  a.customer.descriptive_name Account,
  a.customer.id Customer_ID,
  a.segments.date Date,
  a.customer.currency_code Currency,
  Payment_account_id,
  Payment_account_name,
  Budget_approved,
  Budget_served,
  Budget_start_time,
  Budget_end_time,
  Budget_remain,
  Spending_limit,
  CASE
    WHEN Budget_start_time IS NOT NULL THEN Budget_last
    WHEN Budget_start_time IS NULL THEN 0
    ELSE 0
    END Budget_last_days,
  a.metrics.clicks Clicks,
  a.metrics.impressions Impressions,
  a.metrics.cost_micros / 1e6 Cost,
  a.metrics.conversions Conversions,
  a.metrics.conversions_value Conv_value,
  a.metrics.all_conversions All_conversions,
  a.metrics.all_conversions_value All_conv_value,
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
  week1_impressions AS Week1_impressions,
  week2_impressions AS Week2_impressions,
  cost_wow AS Cost_WOW,
  conversions_wow AS Conversions_WOW,
  AVG(a.customer.optimization_score) Account_optimization_score
FROM `${datasetId}.report_base_account_performance*` a
LEFT JOIN `${datasetId}.nonapp_snd_account_perf_budget` b
  ON
    a.customer.id = b.customer_id
    AND a.segments.date = b.Day
LEFT JOIN `${datasetId}.nonapp_snd_account_perf_cost_wow` w
  ON b.customer_id = w.Customer_id
WHERE date(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
GROUP BY
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
  28, 29, 30, 31, 32, 33, 34