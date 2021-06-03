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
  camp AS (
    SELECT segments_date, camp.*
    FROM
      (
        SELECT DISTINCT segments.date segments_date
        FROM `${datasetId}.report_base_campaign_performance` perf
        INNER JOIN
          (
            SELECT DATE_ADD(DATE(MIN(_PARTITIONTIME)), INTERVAL -1 day) launch_date
            FROM `${datasetId}.report_base_campaigns`
          )
          ON segments.date < launch_date
        WHERE
          DATE(perf._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
          OR perf.segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day)
      )
    LEFT JOIN
      (
        SELECT *
        FROM `${datasetId}.report_base_campaigns`
        WHERE
          _PARTITIONTIME IN (SELECT MIN(_PARTITIONTIME) FROM `${datasetId}.report_base_campaigns`)
      ) camp
      ON 1 = 1
    UNION ALL
    (
      SELECT DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -1 day) AS segments_date, *
      FROM `${datasetId}.report_base_campaigns`
    )
    UNION ALL
    (
      SELECT DATE_ADD(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL -1 day) AS segments_date, *
      FROM `${datasetId}.report_base_campaigns`
      WHERE
        _PARTITIONTIME IN (SELECT MAX(_PARTITIONTIME) FROM `${datasetId}.report_base_campaigns`)
    )
  )
SELECT DISTINCT
  customer.currency_code customer_currency_code,
  DATE_ADD(DATE(camp.segments_date), INTERVAL(2 - EXTRACT(DAYOFWEEK FROM camp.segments_date)) day)
    AS segments_week,
  camp.segments_date segments_date,
  campaign.id campaign_id,
  campaign.advertising_channel_sub_type advertising_channel_sub_type,
  campaign.name campaign_name,
  customer.descriptive_name customer_descriptive_name,
  customer.id customer_id,
  ifnull(adg.ad_groups, 0) ad_groups,
  adg.metrics_cost_total,
  campaign.app_campaign_setting.app_id campaign_app_campaign_setting_app_id,
  ROUND(campaign_budget.amount_micros / 1e6, 2) campaign_budget_amount,
  IF(
    campaign.app_campaign_setting.app_store IS NOT NULL,
    campaign.app_campaign_setting.app_store,
    NULL)
    campaign_app_campaign_setting_app_store,
  IF(
    campaign.app_campaign_setting.bidding_strategy_goal_type IS NOT NULL,
    CASE
      WHEN
        campaign.advertising_channel_sub_type
        = "APP_CAMPAIGN_FOR_ENGAGEMENT"
        THEN "ACe"
      WHEN
        campaign.app_campaign_setting.bidding_strategy_goal_type
        = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST"
        THEN "AC for Install 1.0"
      WHEN
        campaign.app_campaign_setting.bidding_strategy_goal_type
        = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST"
        THEN "AC for Action"
      WHEN
        campaign.app_campaign_setting.bidding_strategy_goal_type
        = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST"
        THEN "AC for Install 2.0"
      WHEN
        campaign.app_campaign_setting.bidding_strategy_goal_type
        = "OPTIMIZE_RETURN_ON_ADVERTISING_SPEND"
        THEN "AC for Value"
      ELSE "Max Conversion"
      END,
    NULL) AS campaign_app_campaign_setting_bidding_strategy_goal_type,
  campaign.status campaign_status,
  IFNULL(campaign.target_roas.target_roas, 0) campaign_target_roas_target_roas,
  IF(
    campaign.target_cpa.target_cpa_micros IS NOT NULL,
    ROUND(campaign.target_cpa.target_cpa_micros / 1e6, 2),
    0)
    campaign_target_cpa_target_cpa,
  CASE
    WHEN
      campaign.app_campaign_setting.bidding_strategy_goal_type
        = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST"
      AND campaign_budget.amount_micros / campaign.target_cpa.target_cpa_micros < 50
      THEN "Budget < 50x CPI"
    WHEN
      campaign.app_campaign_setting.bidding_strategy_goal_type
        = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST"
      AND campaign_budget.amount_micros / campaign.target_cpa.target_cpa_micros < 10
      THEN "Budget < 10x CPA"
    ELSE "PASS"
    END AS budget_excellence_reason,
  CASE
    WHEN download_conversions = "FIREBASE" AND in_app_conversions IN ("", "GOOGLE_PLAY", "FIREBASE")
      THEN TRUE
    WHEN download_conversions = "GOOGLE_PLAY" AND in_app_conversions = "FIREBASE" THEN TRUE
    ELSE FALSE
    END AS firebase_bid,
  CASE
    WHEN
      ARRAY_LENGTH(SPLIT(download_conversions, ",")) > 1
      OR ARRAY_LENGTH(SPLIT(in_app_conversions, ",")) > 1
      THEN TRUE
    WHEN download_conversions = "GOOGLE_PLAY" OR in_app_conversions = "GOOGLE_PLAY" THEN FALSE
    WHEN REPLACE(in_app_conversions, download_conversions, "") = "" THEN FALSE
    ELSE TRUE
    END AS mix_bid
FROM camp
LEFT JOIN
  (
    SELECT
      campaign.id campaign_id,
      segments.date segments_date,
      COUNT(DISTINCT ad_group.id) ad_groups,
      ROUND(SUM(metrics.cost_micros) / 1e6, 2) metrics_cost_total
    FROM `${datasetId}.report_app_ad_group_perf`
    WHERE
      ad_group.status = "ENABLED"
      AND (
        DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
        OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day))
      AND ad_group.status = "ENABLED"
    GROUP BY 1, 2
  ) adg
  ON campaign.id = adg.campaign_id AND adg.segments_date = camp.segments_date
LEFT JOIN
  (
    SELECT
      campaign_id,
      segments_date,
      STRING_AGG(download_conversions, "") download_conversions,
      STRING_AGG(in_app_conversions, "") in_app_conversions
    FROM
      (
        SELECT
          campaign.id campaign_id,
          segments.date segments_date,
          IF(
            segments.conversion_action_category = "DOWNLOAD",
            STRING_AGG(DISTINCT segments.external_conversion_source),
            "")
            download_conversions,
          IF(
            segments.conversion_action_category != "DOWNLOAD",
            STRING_AGG(DISTINCT segments.external_conversion_source),
            "")
            in_app_conversions
        FROM `${datasetId}.report_base_campaign_conversion`
        WHERE
          (
            DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
            OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day))
          AND metrics.conversions > 0
        GROUP BY campaign.id, segments.date, segments.conversion_action_category
      )
    GROUP BY 1, 2
  ) check
  ON campaign.id = check.campaign_id AND check.segments_date = camp.segments_date
WHERE campaign.advertising_channel_type = "MULTI_CHANNEL"