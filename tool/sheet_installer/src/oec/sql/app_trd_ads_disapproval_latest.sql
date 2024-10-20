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

WITH
  approval_status AS (
    SELECT DISTINCT
      cur.*,
      pre.adgroup_approval_status pre_adgroup_approval_status,
      pre.asset_approval_status pre_asset_approval_status,
      pre.adgroup_review_status pre_adgroup_review_status,
      pre.asset_review_status pre_asset_review_status,
      pre.date pre_partitionTime
    FROM
      `${datasetId}.app_snd_ads_approval_all` cur
    LEFT JOIN
      `${datasetId}.app_snd_ads_approval_all` pre
      USING (
        ad_group_id,
        asset_id)
    WHERE
      cur.date = PARSE_DATE(
        '%Y%m%d',
        '${partitionDay}')
      AND pre.date = DATE_ADD(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL -1 DAY)
  )

-- Add asset detailed information & install loss calc.
SELECT
  approval_status.*,
  loss.adg_installs_disapproval,
  loss.adg_installs_limit,
  loss.days,
  asset_link,
  asset_thumbnail,
  asset_name
FROM
  approval_status
LEFT JOIN
  (
    SELECT
      asset_id,
      asset_thumbnail,
      IFNULL(asset_link, asset_name) asset_link,
      IFNULL(youtube_video_title, asset_name) asset_name
    FROM
      `${datasetId}.app_snd_asset_perf_report`
    INNER JOIN
      (
        SELECT asset_id, MAX(segments_date) segments_date
        FROM ads_reports_data_v4.app_snd_asset_perf_report
        GROUP BY 1
      )
      USING (asset_id, segments_date)
    GROUP BY
      1,
      2,
      3,
      4
  ) asset
  USING (asset_id)
LEFT JOIN
  (
    SELECT
      ad_group_id,
      SUM(adg_installs_disapproval) adg_installs_disapproval,
      SUM(adg_installs_limit) adg_installs_limit,
      COUNT(DISTINCT segments_date) days
    FROM
      (
        SELECT
          campaign_id,
          adgroup_id ad_group_id,
          segments_date,
          IF(adgroup_approval_status = "DISAPPROVED", adg_installs, 0) adg_installs_disapproval,
          IF(adgroup_approval_status = "APPROVED_LIMITED", adg_installs, 0) adg_installs_limit,
          RANK() OVER (PARTITION BY campaign_id, adgroup_id ORDER BY segments_date DESC) rank
        FROM
          `${datasetId}.app_snd_ad_group_perf_report` r
        INNER JOIN
          (
            SELECT DISTINCT ad_group_id, adgroup_approval_status
            FROM
              approval_status
            WHERE
              campaign_status = "ENABLED"
              AND adgroup_approval_status IN ("DISAPPROVED", "APPROVED_LIMITED")
          ) s
          ON s.ad_group_id = r.adgroup_id
      )
    WHERE
      rank <= 14
    GROUP BY
      1
  ) loss
  USING (ad_group_id)