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

CREATE TABLE user_last_impr
AS (
  SELECT *
  FROM
    (
      SELECT
        UPPER(impr.device_id_md5) AS device_id,
        impr.campaign_id,
        camp.campaign_name AS campaign_name,
        inventory_type AS inventory,
        creative.video_message.youtube_video_id AS video_id,
        creative.video_message.video_ad_duration AS video_ad_duration,
        ROW_NUMBER() OVER (PARTITION BY impr.device_id_md5 ORDER BY query_id.time_usec DESC) AS rank
      FROM adh.google_ads_impressions_rdid impr
      LEFT JOIN adh.google_ads_adgroupcreative
        USING (ad_group_creative_id)
      LEFT JOIN adh.google_ads_creative AS creative
        USING (creative_id)
      LEFT JOIN adh.google_ads_campaign camp
        USING (campaign_id)
      WHERE creative.video_message.youtube_video_id IS NOT NULL AND device_id_md5 IS NOT NULL
    ) last
);

SELECT
  base.video_id,
  base.campaign_id,
  base.campaign_name,
  base.inventory,
  COUNT(DISTINCT device_id) AS users,
  SUM(IFNULL(day2_retention, 0)) AS day2_retention,
  SUM(IFNULL(day3_retention, 0)) AS day3_retention,
  SUM(IFNULL(day4_retention, 0)) AS day4_retention,
  SUM(IFNULL(day5_retention, 0)) AS day5_retention,
  SUM(IFNULL(day6_retention, 0)) AS day6_retention,
  SUM(IFNULL(day7_retention, 0)) AS day7_retention,
  SUM(IFNULL(day1_revenue, 0)) AS day1_revenue,
  SUM(IFNULL(day2_revenue, 0)) AS day2_revenue,
  SUM(IFNULL(day3_revenue, 0)) AS day3_revenue,
  SUM(IFNULL(day4_revenue, 0)) AS day4_revenue,
  SUM(IFNULL(day5_revenue, 0)) AS day5_revenue,
  SUM(IFNULL(day6_revenue, 0)) AS day6_revenue,
  SUM(IFNULL(day7_revenue, 0)) AS day7_revenue,
  SUM(IFNULL(day14_revenue, 0)) AS day14_revenue,
  SUM(IFNULL(day30_revenue, 0)) AS day30_revenue
FROM tmp.user_last_impr base
INNER JOIN tmp.bi
  USING (device_id)
WHERE base.rank = 1
GROUP BY 1, 2, 3, 4