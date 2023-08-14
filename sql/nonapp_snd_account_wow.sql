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
  customer.id  AS customer_id,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 0 DAY) AS string),
      metrics.cost_micros,
      0)
    / 1000000) AS week1_cost,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string),
      metrics.cost_micros,
      0)
    / 1000000) AS week2_cost,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 0 DAY) AS string),
      metrics.clicks,
      0)) AS week1_clicks,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string),
      metrics.clicks,
      0)) AS week2_clicks,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 0 DAY) AS string),
      metrics.conversions,
      0)) AS week1_conversions,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string),
      metrics.conversions,
      0)) AS week2_conversions,
sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 0 DAY) AS string),
      metrics.impressions,
      0)
    / 1000000) AS week1_impressions,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string),
      metrics.impressions,
      0)
    / 1000000) AS week2_impressions,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 0 DAY) AS string),
      metrics.conversions_value,
      0)) AS week1_conversion_value,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string),
      metrics.conversions_value,
      0)) AS week2_conversion_value,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 0 DAY) AS string),
      metrics.all_conversions_value,
      0)) AS week1_all_conversion_value,
  sum(
    IF(
      CAST(segments.date AS string)
        BETWEEN CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) AS string)
        AND CAST(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) AS string),
      metrics.all_conversions_value,
      0)) AS week2_all_conversion_value
FROM `${datasetId}.report_base_account_performance_*`
WHERE DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
GROUP BY 1