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
    FROM `${datasetId}.${labelSourceTable}`
    GROUP BY id
  )
SELECT DISTINCT
  a.*,
  b.billing_setup.payments_account_info.payments_account_name AS Billing_profile,
  b.billing_setup.payments_account_info.payments_account_id AS Billing_profile_id,
  l.name AS Label
FROM `${datasetId}.${queryName}` a
LEFT JOIN `${datasetId}.report_base_account_budget` b
  ON a.customer_id = b.customer.id
LEFT JOIN AGGLabels l
  ON a.customer_id = l.id
WHERE
  DATE(b._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  AND b.account_budget.approved_start_date_time IS NOT NULL
  AND CAST(b.account_budget.approved_start_date_time AS datetime)
    <= PARSE_DATE('%Y%m%d', '${partitionDay}')
  AND (
    b.account_budget.approved_end_date_time IS NULL
    OR CAST(b.account_budget.approved_end_date_time AS datetime)
      >= PARSE_DATE('%Y%m%d', '${partitionDay}'))
