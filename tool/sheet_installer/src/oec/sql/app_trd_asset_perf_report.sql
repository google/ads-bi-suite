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
  base AS (
    SELECT DISTINCT
      camp.*,
      youtube_video_title,
      adgroup_id,
      adgroup_status,
      segments_ad_network_type,
      youtube_video_asset_youtube_video_id,
      p.asset_name,
      p.adgroup_name,
      p.ad_group_ad_asset_view_performance_label,
      p.ad_group_ad_asset_view_field_type,
      image_asset_full_size_width_pixels,
      image_asset_full_size_height_pixels,
      image_asset_full_size_url,
      asset_id,
      asset_link,
      asset_thumbnail,
      metrics_clicks,
      metrics_conversions_value,
      metrics_impressions,
      metrics_cost,
      metrics_conversions,
      metrics_all_conversions,
      metrics_all_conversions_value,
      metrics_installs,
      metrics_in_app_actions
    FROM
      `${datasetId}.app_snd_asset_perf_report` asset
    INNER JOIN
      (
        SELECT
          adgroup_id,
          asset_id,
          adgroup_name,
          asset_name,
          ad_group_ad_asset_view_performance_label,
          ad_group_ad_asset_view_field_type
        FROM
          `${datasetId}.app_snd_asset_perf_report` raw
        INNER JOIN
          (
            SELECT
              adgroup_id,
              asset_id,
              MAX(segments_date) segments_date,
              MAX(ad_group_ad_asset_view_field_type) ad_group_ad_asset_view_field_type
            FROM
              `${datasetId}.app_snd_asset_perf_report`
            GROUP BY
              1,
              2
          )
          USING (
            adgroup_id,
            asset_id,
            segments_date,
            ad_group_ad_asset_view_field_type)
      ) p
      USING (
        adgroup_id,
        asset_id)
    INNER JOIN
      `${datasetId}.base_snd_campaigns` camp
      USING (
        campaign_id,
        segments_date)
  )
SELECT
  base.*,
  first_serve_day,
  DATE_ADD(LAST_DAY(first_serve_day, WEEK(TUESDAY)), INTERVAL -7 DAY) first_serve_week,
  FORMAT_DATE('%Y%m', first_serve_day) first_serve_month,
  roi roi_overall,
  ipm ipm_overall,
  ctr ctr_overall,
  ipm_percentile,
  roi_percentile,
  ctr_percentile
FROM base
LEFT JOIN
  (
    SELECT
      asset_id,
      campaign_app_campaign_setting_app_id,
      ipm,
      ctr,
      roi,
      campaign_app_campaign_setting_bidding_strategy_goal_type,
      PERCENT_RANK()
        OVER (
          PARTITION BY campaign_app_campaign_setting_app_id, ad_group_ad_asset_view_field_type
          ORDER BY ipm DESC
        ) ipm_percentile,
      PERCENT_RANK()
        OVER (
          PARTITION BY campaign_app_campaign_setting_app_id, ad_group_ad_asset_view_field_type
          ORDER BY roi DESC
        ) roi_percentile,
      PERCENT_RANK()
        OVER (
          PARTITION BY campaign_app_campaign_setting_app_id, ad_group_ad_asset_view_field_type
          ORDER BY ctr DESC
        ) ctr_percentile
    FROM
      (
        SELECT
          asset_id,
          campaign_app_campaign_setting_app_id,
          ad_group_ad_asset_view_field_type,
          campaign_app_campaign_setting_bidding_strategy_goal_type,
          ROUND(SAFE_DIVIDE(SUM(metrics_installs), SUM(metrics_impressions) * 1000), 2) ipm,
          ROUND(SAFE_DIVIDE(SUM(metrics_conversions_value), SUM(metrics_cost)), 2) roi,
          ROUND(SAFE_DIVIDE(SUM(metrics_clicks), SUM(metrics_impressions)), 2) ctr,
        FROM base
        WHERE asset_id IS NOT NULL
        GROUP BY
          1,
          2,
          3,
          4
      )
  ) score
  USING (
    asset_id,
    campaign_app_campaign_setting_app_id,
    campaign_app_campaign_setting_bidding_strategy_goal_type)
LEFT JOIN
  (
    SELECT
      asset_id,
      MIN(segments_date) first_serve_day
    FROM base
    WHERE asset_id IS NOT NULL
    GROUP BY
      1
  )
  USING (asset_id)