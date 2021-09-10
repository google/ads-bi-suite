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
  a.customer.id AS customer_id,
  CAST(a.segments.date AS DATE) AS Day,
  b.billing_setup.payments_account_info.payments_account_id AS Payment_account_id,
  b.billing_setup.payments_account_info.payments_account_name AS Payment_account_name,
  b.account_budget.adjusted_spending_limit_micros / 1000000 AS Budget_approved,
  b.account_budget.amount_served_micros / 1000000 AS Budget_served,
  b.account_budget.approved_start_date_time AS Budget_start_time,
  b.account_budget.approved_end_date_time AS Budget_end_time,
  b.account_budget.proposed_spending_limit_type AS Spending_limit,
  (
    ifnull(CAST(b.account_budget.adjusted_spending_limit_micros AS INT64), 0)
    - b.account_budget.amount_served_micros)
    / 1000000 AS Budget_remain,
  avg(
    (
      ifnull(CAST(b.account_budget.adjusted_spending_limit_micros AS INT64), 0)
      - b.account_budget.amount_served_micros))
    / avg(nullif(a.metrics.cost_micros, 0)) AS Budget_last
FROM `${datasetId}.report_base_account_performance_*` a
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
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10