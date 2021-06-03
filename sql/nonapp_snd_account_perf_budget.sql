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
  a.customer.descriptive_name AS Account,
  a.customer.id AS Customer_ID,
  CAST(a.segments.date AS DATE) AS Day,
  a.customer.currency_code AS Currency,
  a.customer.optimization_score AS Account_optimization_score,
  b.account_budget.adjusted_spending_limit_micros / 1000000 AS Budget_approved,
  b.account_budget.amount_served_micros / 1000000 AS Budget_served,
  b.account_budget.approved_start_date_time AS Budget_start_time,
  b.account_budget.approved_end_date_time AS Budget_end_time,
  (
    ifnull(CAST(b.account_budget.adjusted_spending_limit_micros AS INT64), 0)
    - b.account_budget.amount_served_micros)
    / 1000000 AS Budget_remain,
  avg(
    (
      ifnull(CAST(b.account_budget.adjusted_spending_limit_micros AS INT64), 0)
      - b.account_budget.amount_served_micros))
    / avg(nullif(a.metrics.cost_micros, 0)) AS Budget_last,
  a.metrics.clicks AS clicks,
  a.metrics.impressions AS impressions,
  a.metrics.cost_micros / 1000000 AS cost,
  a.metrics.conversions AS conversions,
  a.metrics.cost_micros / 1000000 / nullif(a.metrics.conversions, 0) AS CPA,
  a.metrics.conversions_value AS Conv_value,
  a.metrics.conversions_value / nullif((a.metrics.cost_micros / 1000000), 0) AS ROI,
  a.metrics.all_conversions AS All_conversions,
  a.metrics.all_conversions_value AS All_conv_value
FROM `${datasetId}.report_base_account_performance` a
LEFT JOIN `${datasetId}.report_base_account_budget` b
  ON a.customer.id = b.customer.id
WHERE
  DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  AND DATE(b._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  AND b.account_budget.approved_start_date_time IS NOT NULL
  AND (
    b.account_budget.approved_end_date_time IS NULL
    OR CAST(b.account_budget.approved_end_date_time AS datetime)
      >= PARSE_DATE('%Y%m%d', '${partitionDay}'))
GROUP BY
  Account, Customer_ID, Day, Currency, Account_optimization_score, Budget_approved, Budget_served,
  Budget_start_time, Budget_end_time, Budget_remain, clicks, impressions, cost, conversions, CPA,
  Conv_value, ROI, All_conversions, All_conv_value