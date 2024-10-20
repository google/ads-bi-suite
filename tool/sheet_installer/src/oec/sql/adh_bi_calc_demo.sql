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

CREATE TABLE demo AS (
  SELECT
    UPPER(device_id_md5) AS device_id,
    b.app_id,
    gender_name as gender_name,
    age_group_name as age_group_name,
    ##
    query_id.time_usec AS query_time,
  FROM
    adh.google_ads_impressions_rdid impr
    LEFT JOIN adh.gender ON demographics.gender = gender_id
    LEFT JOIN adh.age_group ON demographics.age_group = age_group_id
    INNER JOIN `${datasetId}.adh_app_prep_${partitionDay}` b
              ON b.campaign_id = impr.campaign_id
  WHERE
    device_id_md5 IS NOT NULL

);
SELECT
  user_type,
  app_id,
  platform,
  gender_name,
  age_group_name,
  COUNT(distinct device_id) AS users,
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
  INNER JOIN tmp.demo USING (device_id, app_id)
##
WHERE
  UNIX_MICROS(TIMESTAMP(install_date)) >= query_time
GROUP BY
  1,
  2,
  3,
  4,5