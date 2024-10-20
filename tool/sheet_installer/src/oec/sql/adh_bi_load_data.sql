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

CREATE TABLE bi AS (
  SELECT
    UPPER(TO_HEX(MD5(UPPER(device_id)))) AS device_id,
    install_date,
    app_id,
    platform,
    country,
    CASE
      WHEN number_of_installs > 1 THEN 'reinstall user'
      WHEN number_of_installs = 1 THEN 'new user'
      END AS user_type,
    day2_retention,
    day7_retention,
    day30_retention,
    day2_revenue,
    day7_revenue,
    day30_revenue,
    number_of_installs
    FROM
      `${biDatasetId}.${biTableId}_${partitionDay}`
      )