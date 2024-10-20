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

CREATE TABLE affinity AS (
  SELECT
    UPPER(device_id_md5) AS device_id,
    b.app_id,
    affinity_name,
    query_id.time_usec AS query_time
  FROM
    adh.google_ads_impressions_rdid impr,
    UNNEST (affinity) AS affinity_id
    LEFT JOIN adh.affinity USING (affinity_id)
    INNER JOIN `${datasetId}.adh_app_prep_${partitionDay}` b
              ON b.campaign_id = impr.campaign_id
  WHERE
    device_id_md5 IS NOT NULL
);
SELECT
  user_type,
  platform,
  app_id,
  affinity_name,
  COUNT(distinct device_id) AS users,
  COUNT(device_id) AS total_grouped_users,
  SUM(
    IFNULL(day2_retention, 0)
  ) AS day2_retention,
  SUM(
    IFNULL(day7_retention, 0)
  ) AS day7_retention,
  SUM(
    IFNULL(day30_retention, 0)
  ) AS day30_retention,
  SUM(
    IFNULL(day2_revenue, 0)
  ) AS day2_revenue,
  SUM(
    IFNULL(day7_revenue, 0)
  ) AS day7_revenue,
  SUM(
    IFNULL(day30_revenue, 0)
  ) AS day30_revenue
FROM
  tmp.bi
  LEFT JOIN tmp.affinity aff USING (device_id, app_id)
WHERE
  UNIX_MICROS(TIMESTAMP(install_date)) >= query_time
GROUP BY
  1,
  2,
  3, 4