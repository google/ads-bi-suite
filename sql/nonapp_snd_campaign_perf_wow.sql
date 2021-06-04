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
  customer.id AS Customer_id,
  campaign.id AS Campaign_id,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) AS string),
      metrics.cost_micros,
      0)
    / 1000000) AS week1_cost,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) AS string),
      metrics.cost_micros,
      0)
    / 1000000) AS week2_cost,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) AS string),
      metrics.cost_micros,
      0))
      / nullif(
        sum(
          IF(
            CAST(segments.date AS string)
              BETWEEN CAST(
                DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
              AND CAST(
                DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) AS string),
            metrics.cost_micros,
            0)),
        0)
    - 1 AS cost_wow,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) AS string),
      metrics.clicks,
      0)) AS week1_clicks,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) AS string),
      metrics.clicks,
      0)) AS week2_clicks,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) AS string),
      metrics.clicks,
      0))
      / nullif(
        sum(
          IF(
            CAST(segments.date AS string)
              BETWEEN CAST(
                DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
              AND CAST(
                DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) AS string),
            metrics.clicks,
            0)),
        0)
    - 1 AS clicks_wow,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) AS string),
      metrics.conversions,
      0)) AS week1_conversions,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) AS string),
      metrics.conversions,
      0)) AS week2_conversions,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) AS string),
      metrics.conversions,
      0))
      / nullif(
        sum(
          IF(
            CAST(segments.date AS string)
              BETWEEN CAST(
                DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
              AND CAST(
                DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) AS string),
            metrics.conversions,
            0)),
        0)
    - 1 AS conversions_wow,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) AS string),
      metrics.conversions_value,
      0)) AS week1_conversion_value,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) AS string),
      metrics.conversions_value,
      0)) AS week2_conversion_value,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) AS string),
      metrics.all_conversions_value,
      0)) AS week1_all_conversion_value,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) AS string),
      metrics.all_conversions_value,
      0)) AS week2_all_conversion_value
FROM `${datasetId}.report_base_campaign_performance`
WHERE Date(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
GROUP BY Customer_id, Campaign_id