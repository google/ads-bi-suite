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
  p.campaign.id,
  p.campaign.name,
  AVG(p.campaign.optimization_score) AS avg_campaign_optimization_score,
  AVG(p.campaign_budget.amount_micros) / 1000000 AS budget,
  spend_7d,
  safe_divide(AVG(spend_7d), AVG(p.campaign_budget.amount_micros / 1000000)) AS budget_utilization
FROM `${datasetId}.report_base_campaigns` p
LEFT JOIN
  (
    SELECT c.id, c.name, AVG(cost_micros) / 1000000 AS spend_7d,
    FROM
      (
        SELECT campaign.id, campaign.name, segments.date, SUM(metrics.cost_micros) AS cost_micros
        FROM `${datasetId}.report_base_campaign_performance`
        WHERE
          date(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
          AND segments.date < PARSE_DATE('%Y%m%d', '${partitionDay}')
          AND segments.date > date_sub(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 day)
        GROUP BY 1, 2, 3
      ) c
    GROUP BY 1, 2
  ) t
  ON p.campaign.id = t.id
WHERE
  date(_partitionTime) <= PARSE_DATE('%Y%m%d', '${partitionDay}')
  AND date(_partitionTime) > date_sub(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 day)
GROUP BY campaign.id, campaign.name, spend_7d