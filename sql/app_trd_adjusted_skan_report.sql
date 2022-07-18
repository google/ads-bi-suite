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
  base AS (
    SELECT
      campaign.id AS campaign_id,
      segments.date AS segments_date,
      segments.sk_ad_network_conversion_value AS skan_cv,
      segments.sk_ad_network_ad_event_type AS skan_event_type,
      segments.sk_ad_network_source_app.sk_ad_network_source_app_id AS skan_source_app_id,
      segments.sk_ad_network_user_type AS skan_user_type,
      segments.sk_ad_network_attribution_credit AS skan_attribution_credit,
      SUM(metrics.sk_ad_network_conversions) AS skan_conversions
    FROM
      `${datasetId}.report_app_skan` r
    INNER JOIN
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          MAX(_partitionTime) partitionTime
        FROM
          `${datasetId}.report_app_skan`
        GROUP BY
          1,
          2
      ) t
      ON
        t.partitionTime = r._partitionTime
        AND t.campaign_id = r.campaign.id
        AND t.segments_date = r.segments.date
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7
  ),
  perf AS (
    SELECT DISTINCT
      camp.*,
      metrics_cost,
      installs,
      in_app_actions,
      metrics_clicks,
      metrics_impressions,
      metrics_conversions_value,
      metrics_conversions,
      metrics_all_conversions_value,
      metrics_all_conversions
    FROM
      (
        SELECT
          campaign_id,
          segments_date,
          SUM(metrics_cost) metrics_cost,
          SUM(installs) installs,
          SUM(in_app_actions) in_app_actions,
          SUM(metrics_clicks) metrics_clicks,
          SUM(metrics_impressions) metrics_impressions,
          SUM(metrics_conversions_value) metrics_conversions_value,
          SUM(metrics_conversions) metrics_conversions,
          SUM(metrics_all_conversions_value) metrics_all_conversions_value,
          SUM(metrics_all_conversions) metrics_all_conversions
        FROM
          `${datasetId}.base_snd_campaign_performance`
        GROUP BY
          1,
          2
      ) perf
    INNER JOIN
      `${datasetId}.base_snd_campaigns` camp
      USING (
        campaign_id,
        segments_date)
  ),
  raw AS (
    SELECT
      segments_date,
      skan_cv,
      campaign_id,
      SUM(
        IF(
          skan_cv IS NOT NULL,
          skan_conversions,
          IF(
            skan_cv IS NULL
              AND IF(
                skan_attribution_credit <> 'WON'
                  OR skan_attribution_credit IS NULL,
                TRUE,
                FALSE),
            skan_conversions,
            0))) AS conversion
    FROM
      base
    GROUP BY
      1,
      2,
      3
  ),
  cv_base AS (
    SELECT
      segments_date,
      SUM(
        IF(
          skan_cv IS NULL
            AND skan_attribution_credit = 'WON',
          skan_conversions,
          0)) AS null_conv,
      SUM(
        IF(
          skan_cv IS NOT NULL,
          skan_conversions,
          0)) AS conversion
    FROM
      base
    WHERE
      skan_source_app_id <> '544007664'
      OR skan_source_app_id IS NULL
    GROUP BY
      1
  ),
  undistributed AS (
    SELECT
      segments_date,
      skan_cv,
      campaign_id,
      SUM(skan_conversions) AS conversion
    FROM
      base
    WHERE
      skan_source_app_id <> '544007664'
      OR skan_source_app_id IS NULL
    GROUP BY
      1,
      2,
      3
  ),
  distributed AS (
    SELECT
      raw.segments_date,
      raw.skan_cv,
      raw.campaign_id,
      IF(
        raw.skan_cv IS NOT NULL
          AND undistributed.conversion IS NOT NULL,
        raw.conversion + ROUND(undistributed.conversion * cv_base.null_conv / cv_base.conversion),
        raw.conversion) AS skan_null_distributed_cv
    FROM
      raw
    LEFT JOIN
      cv_base
      ON
        raw.segments_date = cv_base.segments_date
    LEFT JOIN
      undistributed
      ON
        undistributed.segments_date = raw.segments_date
        AND undistributed.skan_cv = raw.skan_cv
        AND undistributed.campaign_id = raw.campaign_id
  )
SELECT DISTINCT
  distributed.skan_cv,
  distributed.skan_null_distributed_cv,
  perf.*,
  skans
FROM
  perf
INNER JOIN
  distributed
  USING (
    segments_date,
    campaign_id)
LEFT JOIN
  (
    SELECT
      campaign_id,
      segments_date,
      COUNT(*) skans
    FROM
      distributed
    GROUP BY
      1,
      2
  )
  USING (
    campaign_id,
    segments_date)
ORDER BY
  perf.segments_date,
  distributed.skan_cv;