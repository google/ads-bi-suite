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

WITH
  AGGLabels AS (
    SELECT id, STRING_AGG(name) AS name
    FROM ${datasetId}.${labelSourceTable}
    GROUP BY id
  )
SELECT DISTINCT
  a.*,
  Billing_profile_id,
  Billing_profile,
  rate_usd,
  rate_aud,
  rate_sgd,
  l.name AS Label
FROM ${datasetId}.${queryName} a
LEFT JOIN
  (
    SELECT
      customer.id AS id,
      STRING_AGG(billing_setup.payments_account_info.payments_account_id) AS Billing_profile_id,
      STRING_AGG(billing_setup.payments_account_info.payments_account_name) AS Billing_profile
    FROM ${datasetId}.report_base_account_budget
    WHERE
      DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      AND account_budget.status = "APPROVED"
      AND account_budget.approved_start_date_time IS NOT NULL
      AND CAST(account_budget.approved_start_date_time AS datetime)
        <= PARSE_DATE('%Y%m%d', '${partitionDay}')
      AND (
        account_budget.approved_end_date_time IS NULL
        OR CAST(account_budget.approved_end_date_time AS datetime)
          >= PARSE_DATE('%Y%m%d', '${partitionDay}'))
    GROUP BY 1
  ) b
  ON a.customer_id = b.id
LEFT JOIN AGGLabels l
  ON a.customer_id = l.id
LEFT JOIN
  (
    SELECT DISTINCT
      customer.id customer_id,
      customer.currency_code currency,
      rate AS rate_usd
    FROM
      ${datasetId}.report_base_campaigns c
    LEFT JOIN
      ${datasetId}.fx_rate f
      ON
        customer.currency_code = f.fromcur
    WHERE
      f.tocur = "USD"
      AND date(f._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      AND date(c._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  )
  USING (customer_id)
LEFT JOIN
  (
    SELECT DISTINCT
      customer.id customer_id,
      customer.currency_code currency,
      rate AS rate_aud
    FROM
      ${datasetId}.report_base_campaigns c
    LEFT JOIN
      ${datasetId}.fx_rate f
      ON
        customer.currency_code = f.fromcur
    WHERE
      f.tocur = "AUD"
      AND date(f._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      AND date(c._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  )
  USING (customer_id)
LEFT JOIN
  (
    SELECT DISTINCT
      customer.id customer_id,
      customer.currency_code currency,
      rate AS rate_sgd
    FROM
      ${datasetId}.report_base_campaigns c
    LEFT JOIN
      ${datasetId}.fx_rate f
      ON
        customer.currency_code = f.fromcur
    WHERE
      f.tocur = "SGD"
      AND date(f._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      AND date(c._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  )
  USING (customer_id)