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

CREATE TABLE installed_users
AS (
  SELECT *
  FROM
    (
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
        ON
          b.campaign_id = a.impression_data.campaign_id
          AND b.conversion_id = CAST(a.conversion_type AS string)
      WHERE user_id != '0'
      AND conversion_attribution_model_type = 'LAST_CLICK'
    )
  WHERE rank = 1
);

SELECT
  impr.customer_id,
  impr.campaign_id,
  camp.campaign_name,
  prep.app_id,
  location.country,
  aff.affinity_name,
  aff.affinity_category,
  creative.video_message.youtube_video_id AS video_id,
  creative.video_message.video_ad_duration AS video_ad_duration,
  COUNT(DISTINCT impr.user_id) target_users,
  SUM(IFNULL(impr.advertiser_impression_cost_usd, 0)) + SUM(IFNULL(clk.advertiser_cost_usd, 0)) AS cost_usd,
  SUM(clk.num_clicks) AS clicks,
  COUNT(impr.query_id.time_usec) AS impressions,
  COUNT(DISTINCT conv.user_id) installs
FROM adh.google_ads_impressions impr
CROSS JOIN UNNEST(affinity) AS affinity_id
LEFT JOIN adh.google_ads_campaign camp
  USING (campaign_id)
LEFT JOIN adh.google_ads_creative_conversions clk USING (impression_id)
LEFT JOIN tmp.installed_users conv
  ON impr.impression_id = conv.impression_id
LEFT JOIN adh.affinity aff
  USING (affinity_id)
LEFT JOIN adh.google_ads_adgroup adg
  USING (adgroup_id)
LEFT JOIN adh.google_ads_adgroupcreative
  USING (ad_group_creative_id)
LEFT JOIN adh.google_ads_creative creative
  USING (creative_id)
INNER JOIN `${datasetId}.adh_app_prep_${partitionDay}` prep
  ON prep.campaign_id = impr.campaign_id
WHERE impr.user_id != '0'
AND creative.video_message.youtube_video_id != ''
AND creative.video_message.youtube_video_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9