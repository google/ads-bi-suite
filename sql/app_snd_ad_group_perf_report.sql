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

WITH
  ce AS (
    SELECT
      adgroup_id,
      adgroup_status,
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
          ad_group.status adgroup_status,
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
          ad_group.status,
          segments.date,
          ad_group_ad_asset_view.field_type
      )
    GROUP BY
      1,
      2,
      3
  )
SELECT DISTINCT
  campaign_id,
  segments_date,
  adgroup_id,
  adgroup_name,
  adg.adgroup_status,
  segments_ad_network_type,
  IFNULL(metrics_impressions, 0) adg_impressions,
  IFNULL(metrics_clicks, 0) adg_clicks,
  IFNULL(metrics_cost, 0) adg_cost,
  IFNULL(installs, 0) adg_installs,
  IFNULL(in_app_actions, 0) adg_in_app_actions,
  IFNULL(metrics_conversions_value, 0) adg_conversions_value,
  IFNULL(ce.headline, 0) headline,
  IFNULL(ce.description, 0) description,
  IFNULL(ce.image, 0) image,
  IFNULL(ce.video, 0) video,
  IFNULL(l.headline, 0) headline_l,
  IFNULL(l.description, 0) description_l,
  IFNULL(l.image, 0) image_l,
  IFNULL(l.video, 0) video_l
FROM
  (
    SELECT
      campaign_id,
      adgroup_id,
      adgroup_name,
      adgroup_status,
      segments_date,
      segments_ad_network_type,
      metrics_cost,
      metrics_impressions,
      metrics_clicks,
      metrics_conversions_value,
      installs,
      in_app_actions
    FROM
      (
        SELECT DISTINCT
          campaign.id campaign_id,
          ad_group.id adgroup_id,
          ad_group.name adgroup_name,
          ad_group.status adgroup_status,
          segments.date segments_date,
          segments.ad_network_type segments_ad_network_type,
          ROUND(SUM(metrics.cost_micros) / 1e6, 2) metrics_cost,
          SUM(metrics.impressions) metrics_impressions,
          SUM(metrics.clicks) metrics_clicks,
          SUM(metrics.conversions_value) metrics_conversions_value
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
          2,
          3,
          4,
          5,
          6
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
LEFT JOIN
  ce
  USING (
    adgroup_id,
    segments_date)
LEFT JOIN
  (
    SELECT
      adgroup_id,
      headline,
      image,
      video,
      description
    FROM
      ce
    INNER JOIN
      (
        SELECT
          adgroup_id,
          MAX(segments_date) segments_date
        FROM
          ce
        WHERE
          adgroup_status = "ENABLED"
        GROUP BY
          1
      )
      USING (
        adgroup_id,
        segments_date)
  ) l
  USING (adgroup_id)