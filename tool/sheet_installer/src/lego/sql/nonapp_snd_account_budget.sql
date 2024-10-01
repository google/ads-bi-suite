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
  a.customer.id customer_id,
  a.customer.descriptive_name customer_descriptive_name,
  a.customer.currency_code customer_currency_code,
  CAST(a.segments.date AS DATE) segment_date,
  b.account_budget.adjusted_spending_limit_micros / 1e6 budget_approved,
  b.account_budget.amount_served_micros / 1e6 budget_served,
  b.account_budget.approved_start_date_time budget_start_time,
  CASE
    WHEN b.billing_setup.end_date_time IS NOT NULL THEN b.billing_setup.end_date_time
    ELSE b.account_budget.approved_end_date_time
    END budget_end_time,
  b.account_budget.proposed_spending_limit_type spending_limit,
  b.account_budget.purchase_order_number purchase_order_number,
  (
    ifnull(CAST(b.account_budget.adjusted_spending_limit_micros AS INT64), 0)
    - b.account_budget.amount_served_micros)
    / 1e6 AS budget_remain,
  avg(
    (
      ifnull(CAST(b.account_budget.adjusted_spending_limit_micros AS INT64), 0)
      - b.account_budget.amount_served_micros))
    / avg(nullif(a.metrics.cost_micros, 0)) AS budget_last
FROM `${datasetId}.report_base_account_performance_*` a
LEFT JOIN `${datasetId}.report_base_account_budget` b
  ON a.customer.id = b.customer.id
INNER JOIN
  (
    SELECT
      customer.id customer_id,
      MAX(account_budget.approved_start_date_time) budget_start_time,
    FROM `${datasetId}.report_base_account_budget`
    WHERE
      DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      AND account_budget.status = 'APPROVED'
      AND account_budget.approved_start_date_time IS NOT NULL
      AND CAST(account_budget.approved_start_date_time AS datetime)
        <= PARSE_DATE('%Y%m%d', '${partitionDay}')
      AND (
        billing_setup.end_date_time IS NULL
        OR CAST(billing_setup.end_date_time AS datetime)
          >= PARSE_DATE('%Y%m%d', '${partitionDay}'))
      AND (
        account_budget.approved_end_date_time IS NULL
        OR CAST(account_budget.approved_end_date_time AS datetime)
          >= PARSE_DATE('%Y%m%d', '${partitionDay}'))
    GROUP BY customer_id
  ) max
  ON
    b.customer.id = max.customer_id
    AND b.account_budget.approved_start_date_time = budget_start_time
WHERE
  DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  AND DATE(b._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  AND b.account_budget.status = 'APPROVED'
  AND b.account_budget.approved_start_date_time IS NOT NULL
  AND CAST(b.account_budget.approved_start_date_time AS datetime)
    <= PARSE_DATE('%Y%m%d', '${partitionDay}')
  AND (
    b.billing_setup.end_date_time IS NULL
    OR CAST(b.billing_setup.end_date_time AS datetime)
      >= PARSE_DATE('%Y%m%d', '${partitionDay}'))
  AND (
    b.account_budget.approved_end_date_time IS NULL
    OR CAST(b.account_budget.approved_end_date_time AS datetime)
      >= PARSE_DATE('%Y%m%d', '${partitionDay}'))
GROUP BY
  1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
