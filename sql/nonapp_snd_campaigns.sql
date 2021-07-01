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
  camp AS (
  SELECT
    segments_date,
    camp.*
  FROM (
    SELECT
      DISTINCT segments.date segments_date
    FROM
      `${datasetId}.report_base_campaign_performance` perf
    INNER JOIN (
      SELECT
        DATE_ADD(DATE(MIN(_PARTITIONTIME)), INTERVAL -1 day) launch_date
      FROM
        `${datasetId}.report_base_campaigns` )
    ON
      segments.date < launch_date
    WHERE
      DATE(perf._partitionTime) = PARSE_DATE('%Y%m%d',
        '${partitionDay}')
      OR perf.segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day) )
  LEFT JOIN (
    SELECT
      *
    FROM
      `${datasetId}.report_base_campaigns`
    WHERE
      _PARTITIONTIME IN (
      SELECT
        MIN(_PARTITIONTIME)
      FROM
        `${datasetId}.report_base_campaigns`) ) camp
  ON
    1 = 1
  UNION ALL (
    SELECT
      DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -1 day) AS segments_date,
      *
    FROM
      `${datasetId}.report_base_campaigns` )
  UNION ALL (
    SELECT
      DATE_ADD(PARSE_DATE('%Y%m%d',
          '${partitionDay}'), INTERVAL -1 day) AS segments_date,
      *
    FROM
      `${datasetId}.report_base_campaigns`
    WHERE
      _PARTITIONTIME IN (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `${datasetId}.report_base_campaigns`) ) )

Select * from camp
