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

CREATE TABLE
imp_status as (
  SELECT
    imp.user_id,
    affinity_id,
    adgroup_id,
    count(*) AS imps, sum(advertiser_impression_cost_usd) as imp_cost
  FROM
    adh.google_ads_impressions imp,
    UNNEST (affinity) AS affinity_id
  WHERE imp.user_id != "0"
      AND affinity_id is not null
      AND adgroup_id IN
      (SELECT DISTINCT adGroupId FROM `adh_audience.adh_config_prep_${partitionDay}`)
  GROUP BY 1,2,3
);
CREATE TABLE
click_status as (
  SELECT
      ck.user_id,
      COUNT(*) AS clks,
      COUNT(distinct user_id) as clk_uniques,
      sum(advertiser_click_cost_usd) AS click_cost
    FROM adh.google_ads_clicks ck
    WHERE ck.user_id != "0"
      AND impression_data.adgroup_id IN
      (SELECT DISTINCT adGroupId FROM `adh_audience.adh_config_prep_${partitionDay}`)
  GROUP BY 1
);
CREATE TABLE
conversion AS (
 SELECT
   user_id,
   count(conversion_id.time_usec) AS convs
 FROM
   adh.google_ads_conversions
 WHERE user_id != '0' AND conversion_type IN
 (SELECT DISTINCT conversion_id FROM `adh_audience.adh_config_prep_${partitionDay}`)
 AND impression_data.adgroup_id  IN
 (SELECT DISTINCT adGroupId FROM `adh_audience.adh_config_prep_${partitionDay}`)
 GROUP BY 1
 );
CREATE TABLE
user_aff AS (
  SELECT
    user_id,
    adgroup_id,
    affinity_name, affinity_category
  FROM tmp.imp_status
  LEFT JOIN adh.affinity USING (affinity_id)
  );
CREATE TABLE
user_delivery AS (
  SELECT
    b.user_id,
    SUM(uniques) AS unique_users,
    SUM(clk_uniques) AS clk_unique_users,
    SUM(imps) AS impressions,
    SUM(clks) AS clicks,
    SUM(convs) AS convs_sum,
    SUM(click_cost) AS click_cost_sum,
    SUM(imp_cost) AS imp_cost_sum
  FROM (
    SELECT
      imp.user_id,
      COUNT(DISTINCT imp.user_id) as uniques,
      COUNT(*) AS imps,
      SUM(advertiser_impression_cost_usd) as imp_cost
    FROM
      adh.google_ads_impressions imp
    WHERE imp.user_id != "0"
    GROUP BY 1
    ) b
  LEFT JOIN (
    SELECT * FROM tmp.click_status
    ) USING (user_id)
  LEFT JOIN (
    SELECT * FROM tmp.conversion
    ) USING (user_id)
  GROUP BY 1
  );

SELECT
adgroup_id,
affinity_name as audience_name,
affinity_category as audience_category,
'Affinity' as type,
SUM(impressions) AS impressions,
SUM(clicks) AS clicks,
SUM(unique_users) AS unique_users,
SUM(clk_unique_users) AS clk_unique_users,
SUM(convs_sum) AS convs_sum_users,
SUM(click_cost_sum) AS click_cost_sum_users,
SUM(imp_cost_sum) AS imp_cost_sum_users
FROM tmp.user_aff t
LEFT JOIN
  tmp.user_delivery c3 ON t.user_id = c3.user_id
GROUP BY 1,2,3