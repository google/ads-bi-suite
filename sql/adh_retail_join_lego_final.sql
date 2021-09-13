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
 d.adgroup_id,
 d.type,
 d.audience_name,
 d.audience_category,
 d.impressions,
 d.clicks,
 d.unique_users,
 d.clk_unique_users,
 d.convs_sum_users,
 d.click_cost_sum_users,
 d.imp_cost_sum_users,
 d.adGroupLabel as adgroup_label,
 d.status,
 exists_in_other_adgroup
FROM
 `${datasetId}.adh_retail_join_lego_init_${partitionDay}` AS d
LEFT JOIN (
 SELECT
   DISTINCT *
 FROM (
   SELECT
     a.*,
     b.ad_group_id AS exists_in_other_adgroup
   FROM (
     SELECT
       *
     FROM
       `${datasetId}.adh_retail_join_lego_init_${partitionDay}`
     WHERE
       status IS NULL) AS a
   LEFT JOIN
     `${datasetId}.adh_retail_join_lego_init_${partitionDay}` AS b
   ON
     a.adGroupLabel = b.adGroupLabel
     AND a.audience_name = b.audience_name
     AND a.audience_category = b.audience_category
     AND b.ad_group_id IS NOT NULL )
 WHERE
   exists_in_other_adgroup != adgroup_id ) AS c
ON
 c.adgroup_id = d.adgroup_id
 AND c.audience_name = d.audience_name
 AND c.audience_category = d.audience_category