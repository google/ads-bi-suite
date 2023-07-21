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

-- Check if the given Skan CV is null or not.
--
-- @param skan_cv
-- @param BOOL.
CREATE TEMP FUNCTION SkanCvIsNull(skan_cv INT64)
RETURNS BOOL
AS (
  skan_cv IS NULL
);

-- Check if the given IOS App ID is in scope.
--
-- @param skan_cv
-- @param BOOL.
CREATE TEMP FUNCTION ValidSkanCvSourceAppId(skan_source_app_id STRING)
RETURNS BOOL
AS (
  skan_source_app_id != '544007664'  -- YouTube IOS App ID
  OR skan_source_app_id IS NULL
);

WITH
  LastCampaignDate AS (
    SELECT
      campaign.id AS campaign_id,
      segments.date AS segments_date,
      MAX(_partitionTime) AS partitionTime
    FROM `${datasetId}.report_app_skan`
    GROUP BY
      campaign_id,
      segments_date
  ),
  CampaignAdsInfo AS (
    SELECT
      segments_week,
      customer_descriptive_name,
      customer_id,
      currency,
      segments_date,
      campaign_id,
      advertising_channel_sub_type,
      advertising_channel_type,
      campaign_name,
      campaign_status,
      campaign_bidding_strategy_type,
      campaign_app_campaign_setting_app_id,
      campaign_app_campaign_setting_app_store,
      campaign_app_campaign_setting_bidding_strategy_goal_type,
      language_name,
      language_code,
      country_code,
      country_name,
      url_expansion_opt_out,
      limited_by_budget,
      merchant_id,
      campaign_budget_amount,
      campaign_target_roas_target_roas,
      campaign_target_cpa_target_cpa,
      campaign_optimization_score
    FROM `${datasetId}.base_snd_campaigns`
  ),
  CampaignAdsPerfByDate AS (
    SELECT
      campaign_id,
      segments_date,
      SUM(metrics_cost) AS metrics_cost,
      SUM(installs) AS installs,
      SUM(in_app_actions) AS in_app_actions,
      SUM(metrics_clicks) AS metrics_clicks,
      SUM(metrics_impressions) AS metrics_impressions,
      SUM(metrics_conversions_value) AS metrics_conversions_value,
      SUM(metrics_conversions) AS metrics_conversions,
      SUM(metrics_all_conversions_value) AS metrics_all_conversions_value,
      SUM(metrics_all_conversions) AS metrics_all_conversions
    FROM
      `${datasetId}.base_snd_campaign_performance`
    GROUP BY
      campaign_id,
      segments_date
  ),
  CampaignAdsAllByDate AS (
    SELECT
      segments_date,
      campaign_id,
      segments_week,
      customer_descriptive_name,
      customer_id,
      currency,
      advertising_channel_sub_type,
      advertising_channel_type,
      campaign_name,
      campaign_status,
      campaign_bidding_strategy_type,
      campaign_app_campaign_setting_app_id,
      campaign_app_campaign_setting_app_store,
      campaign_app_campaign_setting_bidding_strategy_goal_type,
      language_name,
      language_code,
      country_code,
      country_name,
      url_expansion_opt_out,
      limited_by_budget,
      merchant_id,
      campaign_budget_amount,
      campaign_target_roas_target_roas,
      campaign_target_cpa_target_cpa,
      campaign_optimization_score,
      metrics_cost,
      installs,
      in_app_actions,
      metrics_clicks,
      metrics_impressions,
      metrics_conversions_value,
      metrics_conversions,
      metrics_all_conversions_value,
      metrics_all_conversions
    FROM CampaignAdsPerfByDate
    INNER JOIN CampaignAdsInfo
      USING (campaign_id, segments_date)
  ),
  CampaignSkanCvByDate AS (
    SELECT
      campaign.id AS campaign_id,
      segments.date AS segments_date,
      segments.sk_ad_network_conversion_value AS skan_cv,
      segments.sk_ad_network_ad_event_type AS skan_event_type,
      segments.sk_ad_network_source_app.sk_ad_network_source_app_id AS skan_source_app_id,
      segments.sk_ad_network_user_type AS skan_user_type,
      segments.sk_ad_network_attribution_credit AS skan_attribution_credit,
      SUM(metrics.sk_ad_network_conversions) AS skan_conversion_counts
    FROM `${datasetId}.report_app_skan` AS AppSkan
    INNER JOIN LastCampaignDate
      ON (
        AppSkan._partitionTime = LastCampaignDate.partitionTime
        AND AppSkan.campaign.id = LastCampaignDate.campaign_id
        AND AppSkan.segments.date = LastCampaignDate.segments_date)
    GROUP BY
      campaign_id,
      segments_date,
      skan_cv,
      skan_event_type,
      skan_source_app_id,
      skan_user_type,
      skan_attribution_credit
  ),
  -- AdMob Won-null installs/admob install
  CampaignNonNullSkanCvYouTubeExcludedByDate AS (
    SELECT
      segments_date,
      campaign_id,
      skan_cv,
      COUNT(*) AS distributed_denominator
    FROM CampaignSkanCvByDate
    WHERE
      ValidSkanCvSourceAppId(skan_source_app_id)
      AND NOT SkanCvIsNull(skan_cv)
      AND skan_attribution_credit = 'WON'
    GROUP BY
      segments_date,
      campaign_id,
      skan_cv
  ),
  CampaignSkanCvWonButNullByDate AS (
    SELECT
      segments_date,
      campaign_id,
      SUM(
        IF(
          skan_attribution_credit = 'WON',
          skan_conversion_counts,
          0)) AS won_but_null_skan_conversion_counts
    FROM CampaignSkanCvByDate
    WHERE
      ValidSkanCvSourceAppId(skan_source_app_id)
      AND SkanCvIsNull(skan_cv)
    GROUP BY
      segments_date,
      campaign_id
  ),
  CampaignDistributedSkanCvByDate AS (
    SELECT
      segments_date,
      campaign_id,
      skan_cv,
      SUM(skan_conversion_counts) AS conversion_numerator
    FROM CampaignSkanCvByDate
    WHERE
      ValidSkanCvSourceAppId(skan_source_app_id)
      -- If Skan CV is not null, then the data is already distributed.
      AND NOT SkanCvIsNull(skan_cv)
    GROUP BY
      segments_date,
      campaign_id,
      skan_cv
  ),
  CampaignDistributedSkanCvByDateAtCampaignLevel AS (
    SELECT
      segments_date,
      campaign_id,
      SUM(conversion_numerator) AS conversion_denominator
    FROM CampaignDistributedSkanCvByDate
    GROUP BY
      segments_date,
      campaign_id
  ),
  CampaignSkanCvWithReDistributedSkanCvByDate AS (
    SELECT
      campaign_id,
      segments_date,
      skan_cv,
      CampaignSkanCvByDate.skan_event_type,
      CampaignSkanCvByDate.skan_source_app_id,
      CampaignSkanCvByDate.skan_user_type,
      CampaignSkanCvByDate.skan_attribution_credit,
      CampaignSkanCvByDate.skan_conversion_counts,
      CampaignSkanCvWonButNullByDate.won_but_null_skan_conversion_counts,
      CampaignDistributedSkanCvByDate.conversion_numerator,
      CampaignDistributedSkanCvByDateAtCampaignLevel.conversion_denominator,
      CampaignNonNullSkanCvYouTubeExcludedByDate.distributed_denominator,
      CASE
        -- CampaignSkanCvWonButNullByDate
        WHEN
          SkanCvIsNull(skan_cv)
          AND ValidSkanCvSourceAppId(skan_source_app_id)
          AND skan_attribution_credit = 'WON'
          THEN 0
        WHEN
          NOT SkanCvIsNull(skan_cv)
          AND ValidSkanCvSourceAppId(skan_source_app_id)
          AND skan_attribution_credit = 'WON'
          THEN
            CampaignSkanCvByDate.skan_conversion_counts
            + CEIL(
              IFNULL(
                SAFE_DIVIDE(
                  CampaignSkanCvWonButNullByDate.won_but_null_skan_conversion_counts
                    * CampaignDistributedSkanCvByDate.conversion_numerator,
                  CampaignDistributedSkanCvByDateAtCampaignLevel.conversion_denominator
                    * CampaignNonNullSkanCvYouTubeExcludedByDate.distributed_denominator),
                0))
        ELSE CampaignSkanCvByDate.skan_conversion_counts
        END AS skan_null_distributed_conversion_counts
    FROM CampaignSkanCvByDate
    LEFT JOIN CampaignSkanCvWonButNullByDate
      USING (segments_date, campaign_id)
    LEFT JOIN CampaignDistributedSkanCvByDate
      USING (segments_date, campaign_id, skan_cv)
    LEFT JOIN CampaignDistributedSkanCvByDateAtCampaignLevel
      USING (segments_date, campaign_id)
    LEFT JOIN CampaignNonNullSkanCvYouTubeExcludedByDate
      USING (segments_date, campaign_id, skan_cv)
  ),
  CampaignSkanCvRawCounts AS (
    SELECT
      campaign_id,
      segments_date,
      COUNT(*) AS raw_counts
    FROM
      CampaignSkanCvWithReDistributedSkanCvByDate
    GROUP BY
      campaign_id,
      segments_date
  )
SELECT DISTINCT
  * EXCEPT (
    metrics_cost,
    installs,
    in_app_actions,
    metrics_clicks,
    metrics_impressions,
    metrics_conversions_value,
    metrics_conversions,
    metrics_all_conversions_value,
    metrics_all_conversions),
  SAFE_DIVIDE(CampaignAdsAllByDate.metrics_cost, CampaignSkanCvRawCounts.raw_counts)
    AS metrics_cost,
  SAFE_DIVIDE(CampaignAdsAllByDate.installs, CampaignSkanCvRawCounts.raw_counts) AS installs,
  SAFE_DIVIDE(CampaignAdsAllByDate.in_app_actions, CampaignSkanCvRawCounts.raw_counts)
    AS in_app_actions,
  SAFE_DIVIDE(CampaignAdsAllByDate.metrics_clicks, CampaignSkanCvRawCounts.raw_counts)
    AS metrics_clicks,
  SAFE_DIVIDE(CampaignAdsAllByDate.metrics_impressions, CampaignSkanCvRawCounts.raw_counts)
    AS metrics_impressions,
  SAFE_DIVIDE(CampaignAdsAllByDate.metrics_conversions_value, CampaignSkanCvRawCounts.raw_counts)
    AS metrics_conversions_value,
  SAFE_DIVIDE(CampaignAdsAllByDate.metrics_conversions, CampaignSkanCvRawCounts.raw_counts)
    AS metrics_conversions,
  SAFE_DIVIDE(
    CampaignAdsAllByDate.metrics_all_conversions_value, CampaignSkanCvRawCounts.raw_counts)
    AS metrics_all_conversions_value,
  SAFE_DIVIDE(CampaignAdsAllByDate.metrics_all_conversions, CampaignSkanCvRawCounts.raw_counts)
    AS metrics_all_conversions
FROM CampaignAdsAllByDate
INNER JOIN CampaignSkanCvWithReDistributedSkanCvByDate
  USING (segments_date, campaign_id)
LEFT JOIN CampaignSkanCvRawCounts
  USING (segments_date, campaign_id)
ORDER BY
  segments_date,
  skan_cv
