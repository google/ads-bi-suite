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

CREATE TABLE installed_users
AS (
  SELECT *
            FROM (
              SELECT
                impression_id,
                b.app_id,
                user_id,
                ROW_NUMBER()
                  OVER (
                    PARTITION BY user_id, b.app_id
                    ORDER BY query_id.time_usec DESC
                  ) AS rank
              FROM adh.google_ads_conversions a
              INNER JOIN `${datasetId}.adh_app_prep_${partitionDay}` b
              ON b.campaign_id = a.impression_data.campaign_id
              AND b.conversion_id = cast(a.conversion_type as string)
              WHERE user_id IS NOT NULL
      )
     WHERE rank = 1
);

SELECT
  impr.customer_id,
  impr.campaign_id,
  camp.campaign_name,
  gender_name as gender_id,
  age_group_name,
  prep.app_id,
  location.country,
  creative.video_message.youtube_video_id as video_id,
  creative.video_message.video_ad_duration as video_ad_duration,
  COUNT(impr.query_id.time_usec) as impressions,
  SUM(clk.click_count) as clicks,
  COUNT(DISTINCT impr.user_id) as targeted_users,
  SUM(IFNULL(impr.advertiser_impression_cost_usd, 0)+IFNULL(clk.click_cost_usd, 0)) AS cost_usd,
  COUNT(DISTINCT conv.user_id) installs
FROM adh.google_ads_impressions impr
  LEFT JOIN adh.google_ads_campaign camp using(campaign_id)
  LEFT JOIN adh.google_ads_adgroup adg USING(adgroup_id)
  LEFT JOIN (
    select
      impression_id,
      count(*) click_count,
      SUM(IFNULL(c.advertiser_click_cost_usd, 0)) click_cost_usd
    from adh.google_ads_clicks c
    group by 1) clk using(impression_id)
  LEFT JOIN adh.gender ON demographics.gender = gender_id
  LEFT JOIN adh.age_group ON demographics.age_group = age_group_id
  LEFT JOIN adh.google_ads_adgroupcreative USING (ad_group_creative_id)
  LEFT JOIN adh.google_ads_creative creative USING (creative_id)
  LEFT JOIN tmp.installed_users conv using(impression_id)
  LEFT JOIN `${datasetId}.adh_app_prep_${partitionDay}` prep ON prep.campaign_id = impr.campaign_id
  WHERE impr.user_id IS NOT NULL
GROUP BY 1,2,3,4,5,6,7,8,9