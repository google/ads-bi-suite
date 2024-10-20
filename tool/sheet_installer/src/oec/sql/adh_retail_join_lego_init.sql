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
 *
FROM ( (
   SELECT *
   FROM
     `${datasetId}.adh_retail_calc_in_market_${partitionDay}`
   WHERE
     convs_sum_users > 0)
 UNION ALL (
   SELECT *
   FROM
     `${datasetId}.adh_retail_calc_affinity_${partitionDay}`
   WHERE
     convs_sum_users > 0) ) AS a
LEFT JOIN
 `${datasetId}.adh_config_prep_${partitionDay}` AS b
ON
 a.adgroup_id = b.adGroupId
LEFT JOIN
 `${datasetId}.adh_retail_prep_${partitionDay}` AS c
ON
 a.adgroup_id = c.Ad_group_id
 AND a.audience_name = c.User_interest
WHERE
 a.adgroup_id IS NOT NULL