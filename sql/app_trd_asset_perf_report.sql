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

SELECT DISTINCT
  camp.*,
  IF(
    asset.video_title IS NULL
      OR asset.video_title = "",
    v.video_title,
    asset.video_title)
    video_title,
  v.video_duration_millis,
  adgroup_id,
  adgroup_name,
  adgroup_status,
  segments_ad_network_type,
  asset_youtube_video_asset_youtube_video_id,
  ad_group_ad_asset_view_performance_label,
  asset_image_asset_full_size_width_pixels,
  asset_image_asset_full_size_height_pixels,
  asset_image_asset_full_size_url,
  asset_name,
  asset_id,
  ad_group_ad_asset_view_field_type,
  asset_link,
  asset_thumbnail,
  network.metrics_clicks,
  network.metrics_conversions_value,
  network.metrics_impressions,
  network.metrics_cost,
  network.metrics_conversions,
  network.metrics_all_conversions,
  network.metrics_all_conversions_value,
  headline,
  image,
  description,
  video,
  adg_records,
  adg.metrics_impressions adg_impressions,
  adg.metrics_clicks adg_clicks,
  adg.metrics_cost adg_cost,
  adg.installs adg_installs,
  adg.in_app_actions adg_in_app_actions,
  adg.metrics_conversions_value adg_conversions_value,
  adg.metrics_all_conversions adg_all_conversions,
  adg.metrics_all_conversions_value adg_all_conversions_value
FROM
  (
    SELECT
      campaign.id campaign_id,
      segments.date segments_date,
      ad_group.id adgroup_id,
      ad_group.name adgroup_name,
      ad_group.status adgroup_status,
      segments.ad_network_type segments_ad_network_type,
      asset.youtube_video_asset.youtube_video_id asset_youtube_video_asset_youtube_video_id,
      ad_group_ad_asset_view.performance_label ad_group_ad_asset_view_performance_label,
      asset.image_asset.full_size.width_pixels asset_image_asset_full_size_width_pixels,
      asset.image_asset.full_size.height_pixels asset_image_asset_full_size_height_pixels,
      asset.image_asset.full_size.url asset_image_asset_full_size_url,
      CASE
        WHEN ad_group_ad_asset_view.field_type IN ("HEADLINE", "DESCRIPTION", "MANDATORY_AD_TEXT")
          THEN asset.text_asset.text
        ELSE
          asset.name
        END
        AS asset_name,
      asset.id asset_id,
      ad_group_ad_asset_view.field_type ad_group_ad_asset_view_field_type,
      CASE
        WHEN ad_group_ad_asset_view.field_type = "YOUTUBE_VIDEO"
          THEN
            CONCAT("https://www.youtube.com/watch?v=", asset.youtube_video_asset.youtube_video_id)
        WHEN ad_group_ad_asset_view.field_type = "MARKETING_IMAGE"
          THEN asset.image_asset.full_size.url
        ELSE
          NULL
        END
        AS asset_link,
      CASE
        WHEN ad_group_ad_asset_view.field_type = "YOUTUBE_VIDEO"
          THEN
            CONCAT(
              "https://i.ytimg.com/vi/",
              asset.youtube_video_asset.youtube_video_id,
              "/hqdefault.jpg")
        WHEN ad_group_ad_asset_view.field_type = "MARKETING_IMAGE"
          THEN asset.image_asset.full_size.url
        ELSE
          NULL
        END
        AS asset_thumbnail,
      SUM(metrics.clicks) metrics_clicks,
      SUM(metrics.conversions_value) metrics_conversions_value,
      SUM(metrics.impressions) metrics_impressions,
      ROUND(SUM(metrics.cost_micros) / 1e6, 2) metrics_cost,
      SUM(metrics.conversions) metrics_conversions,
      SUM(metrics.all_conversions) metrics_all_conversions,
      SUM(metrics.all_conversions_value) metrics_all_conversions_value
    FROM
      `${datasetId}.report_app_asset_performance` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          MAX(DATE(_partitionTime)) partitionTime
        FROM
          `${datasetId}.report_app_asset_performance`
        GROUP BY
          1,
          2
      ) t
      ON
        t.partitionTime = DATE(r._partitionTime)
        AND t.campaign_id = r.campaign.id
        AND t.segments_date = r.segments.date
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16
  ) network
LEFT JOIN
  (
    SELECT
      *
    FROM
      (
        SELECT DISTINCT
          video.id asset_youtube_video_asset_youtube_video_id,
          video.title video_title,
          video.duration_millis video_duration_millis,
          ROW_NUMBER() OVER (PARTITION BY video.id ORDER BY DATETIME(_partitionTime) DESC) row_num
        FROM
          `${datasetId}.report_base_videos`
        WHERE
          DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      )
    WHERE
      row_num = 1
  ) v
  USING (asset_youtube_video_asset_youtube_video_id)
LEFT JOIN
  (
    SELECT
      *
    FROM
      (
        SELECT DISTINCT
          asset.youtube_video_asset.youtube_video_id asset_youtube_video_asset_youtube_video_id,
          asset.youtube_video_asset.youtube_video_title video_title,
          ROW_NUMBER()
            OVER (
              PARTITION BY asset.youtube_video_asset.youtube_video_id
              ORDER BY DATETIME(_partitionTime) DESC
            ) row_num
        FROM
          `${datasetId}.report_app_asset_metadata`
        WHERE
          DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      )
    WHERE
      row_num = 1
  ) asset
  USING (asset_youtube_video_asset_youtube_video_id)
LEFT JOIN
  (
    SELECT
      adgroup_id,
      segments_date,
      SUM(adg_records) adg_records,
      SUM(headline) headline,
      SUM(description) description,
      SUM(image) image,
      SUM(video) video
    FROM
      (
        SELECT
          ad_group.id adgroup_id,
          segments.date segments_date,
          COUNT(*) adg_records,
          IF(ad_group_ad_asset_view.field_type = "HEADLINE", COUNT(DISTINCT asset.id), 0) headline,
          IF(
            ad_group_ad_asset_view.field_type = "DESCRIPTION",
            COUNT(DISTINCT asset.id),
            0)
            description,
          IF(
            ad_group_ad_asset_view.field_type = "MARKETING_IMAGE",
            COUNT(DISTINCT asset.id),
            0)
            image,
          IF(ad_group_ad_asset_view.field_type = "YOUTUBE_VIDEO", COUNT(DISTINCT asset.id), 0) video
        FROM
          `${datasetId}.report_app_asset_performance` r
        INNER JOIN
          (
            SELECT
              campaign.id campaign_id,
              segments.date segments_date,
              MAX(DATE(_partitionTime)) partitionTime
            FROM
              `${datasetId}.report_app_asset_performance`
            GROUP BY
              1,
              2
          ) t
          ON
            t.partitionTime = DATE(r._partitionTime)
            AND t.campaign_id = r.campaign.id
            AND t.segments_date = r.segments.date
        GROUP BY
          ad_group.id,
          segments.date,
          ad_group_ad_asset_view.field_type
      )
    GROUP BY
      1,
      2
  ) g
  USING (
    adgroup_id,
    segments_date)
LEFT JOIN
  (
    SELECT
      adgroup_id,
      segments_date,
      metrics_cost,
      metrics_impressions,
      metrics_clicks,
      metrics_conversions_value,
      metrics_all_conversions,
      metrics_all_conversions_value,
      installs,
      in_app_actions
    FROM
      (
        SELECT DISTINCT
          ad_group.id adgroup_id,
          segments.date segments_date,
          ROUND(SUM(metrics.cost_micros) / 1e6, 2) metrics_cost,
          SUM(metrics.impressions) metrics_impressions,
          SUM(metrics.clicks) metrics_clicks,
          SUM(metrics.conversions_value) metrics_conversions_value,
          SUM(metrics.all_conversions) metrics_all_conversions,
          SUM(metrics.all_conversions_value) metrics_all_conversions_value,
        FROM
          `${datasetId}.report_app_ad_group_perf` r
        INNER JOIN
          (
            SELECT
              campaign.id campaign_id,
              segments.date segments_date,
              MAX(DATE(_partitionTime)) partitionTime
            FROM
              `${datasetId}.report_app_ad_group_perf`
            GROUP BY
              1,
              2
          ) t
          ON
            t.partitionTime = DATE(r._partitionTime)
            AND t.campaign_id = r.campaign.id
            AND t.segments_date = r.segments.date
        GROUP BY
          1,
          2
      )
    LEFT JOIN
      (
        SELECT
          ad_group.id adgroup_id,
          segments.date segments_date,
          SUM(
            IF(
              segments.conversion_action_category = "DOWNLOAD",
              metrics.conversions,
              0))
            installs,
          SUM(
            IF(
              segments.conversion_action_category != "DOWNLOAD",
              metrics.conversions,
              0))
            in_app_actions
        FROM
          `${datasetId}.report_app_ad_group` r
        INNER JOIN
          (
            SELECT
              ad_group.id ad_group_id,
              segments.date segments_date,
              MAX(DATE(_partitionTime)) partitionTime
            FROM
              `${datasetId}.report_app_ad_group`
            GROUP BY
              1,
              2
          ) t
          ON
            t.partitionTime = DATE(r._partitionTime)
            AND t.ad_group_id = r.ad_group.id
            AND t.segments_date = r.segments.date
        GROUP BY
          1,
          2
      )
      USING (
        adgroup_id,
        segments_date)
  ) adg
  USING (
    adgroup_id,
    segments_date)
INNER JOIN
  `${datasetId}.app_snd_campaigns` camp
  USING (
    campaign_id,
    segments_date)