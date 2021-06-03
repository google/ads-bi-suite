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

CREATE TABLE bi
AS (
  SELECT
    UPPER(TO_HEX(MD5(UPPER(device_id)))) AS device_id,
    platform,
    CASE
      WHEN number_of_installs > 1 THEN 'reinstall user'
      WHEN number_of_installs = 1 THEN 'new user'
      END AS user_type,
    day2_retention,
    day3_retention,
    day4_retention,
    day5_retention,
    day6_retention,
    day7_retention,
    day1_revenue,
    day2_revenue,
    day3_revenue,
    day4_revenue,
    day5_revenue,
    day6_revenue,
    day7_revenue,
    day14_revenue,
    day30_revenue,
    number_of_installs
  FROM `${datasetId}.${firebaseTableName}`
)