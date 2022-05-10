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
  rec.*,
  r.metrics.cost_micros,
  r.metrics.impressions,
  r.metrics.clicks,
  r.metrics.conversions,
  r.metrics.conversions_value,
  r.metrics.all_conversions,
  r.metrics.all_conversions_value,
  r.metrics.video_views,
  r.customer.id,
  r.customer.currency_code,
  r.customer.descriptive_name,
  r.customer.optimization_score,
  r.customer.optimization_score_weight
FROM
  `${datasetId}.report_base_account_performance_*` r
INNER JOIN
  (
    SELECT
      customer.id customer_id,
      segments.date segments_date,
      MAX(_PARTITIONTIME) PARTITIONTIME
    FROM
      `${datasetId}.report_base_account_performance_*`
    GROUP BY
      1, 2
  ) rec
  ON
    r.customer.id = rec.customer_id
    AND r.segments.date = rec.segments_date
    AND r._PARTITIONTIME = rec.PARTITIONTIME
