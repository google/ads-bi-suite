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


-- Calc. best practice metrics - check low bid & budget constrianed during last 14 days
WITH
  bp_budget AS (
    SELECT
      base.campaign_id,
      r.segments_date,
      COUNT(DISTINCT IF(low_bid != "PASS", base.segments_date, NULL)) low_bid_days,
      COUNT(
        DISTINCT
          IF(
            budget_constrained IS TRUE,
            base.segments_date,
            NULL))
        budget_constrained_days
    FROM
      (
        SELECT
          campaign_id,
          segments_date,
          -- Budget constrained is defined as: a campaign is currently (data refreshed day) budget constrained
          -- and more than half of the last 14 days is budget constrained.
          IF(
            campaign_status = "ENABLED"
              AND campaign_budget_amount <= metrics_cost,
            TRUE,
            FALSE)
            budget_constrained,
          -- Low bid  is defined as: a campaign is currently (data refreshed day) bid low compared with its actual CPI/CPA/ROAS;
          -- Low bid is more than continuous 4 days of the last 14 days
          CASE
            WHEN
              campaign_app_campaign_setting_bidding_strategy_goal_type
              IN ("AC for Install 1.0", "AC for Install 2.0")
              THEN
                IF(
                  campaign_status = "ENABLED"
                    AND ROUND(SAFE_DIVIDE(metrics_cost, installs), 2)
                      > campaign_target_cpa_target_cpa,
                  "CPI > tCPI",
                  "PASS")
            WHEN campaign_app_campaign_setting_bidding_strategy_goal_type = "AC for Action"
              THEN
                IF(
                  campaign_status = "ENABLED"
                    AND ROUND(SAFE_DIVIDE(metrics_cost, in_app_actions), 2)
                      > campaign_target_cpa_target_cpa,
                  "CPA > tCPA",
                  "PASS")
            WHEN campaign_app_campaign_setting_bidding_strategy_goal_type = "AC for Value"
              THEN
                IF(
                  campaign_status = "ENABLED"
                    AND ROUND(SAFE_DIVIDE(metrics_conversions_value, metrics_cost), 2)
                      > campaign_target_roas_target_roas,
                  "ROAS > tROAS",
                  "PASS")
            ELSE
              "PASS"
            END
            AS low_bid
        FROM
          (
            SELECT
              campaign_id,
              segments_date,
              SUM(metrics_cost) metrics_cost,
              SUM(installs) installs,
              SUM(in_app_actions) in_app_actions,
              SUM(metrics_conversions_value) metrics_conversions_value
            FROM
              `${datasetId}.base_snd_campaign_performance`
            GROUP BY
              1,
              2
          ) raw
        INNER JOIN
          `${datasetId}.base_snd_campaigns` camp
          USING (
            campaign_id,
            segments_date)
      ) base
    INNER JOIN
      (
        -- Check the performance during the last 14 days
        SELECT DISTINCT
          campaign_id,
          segments_date,
          DATE_ADD(segments_date, INTERVAL -13 day) pre_date,
        FROM
          `${datasetId}.base_snd_campaign_performance`
      ) r
      ON
        r.campaign_id = base.campaign_id
        AND base.segments_date
          BETWEEN r.pre_date
          AND r.segments_date
    GROUP BY
      campaign_id,
      segments_date
  )
-- Campaign Performance Report:
SELECT DISTINCT
  camp.*,
  CASE
    WHEN
      campaign_app_campaign_setting_bidding_strategy_goal_type
        IN ("AC for Install 1.0", "AC for Install 2.0")
      AND SAFE_DIVIDE(campaign_budget_amount, campaign_target_cpa_target_cpa) < 50
      THEN "Budget < 50x CPI"
    WHEN
      campaign_app_campaign_setting_bidding_strategy_goal_type = "AC for Action"
      AND SAFE_DIVIDE(campaign_budget_amount, campaign_target_cpa_target_cpa) < 10
      THEN "Budget < 10x CPA"
    ELSE
      "PASS"
    END
    AS budget_excellence_reason,
  CASE
    WHEN
      ifnull(download_conversions, in_app_conversions) = "FIREBASE"
      AND (in_app_conversions IN ("GOOGLE_PLAY", "FIREBASE") OR in_app_conversions IS NULL)
      THEN TRUE
    WHEN
      ifnull(download_conversions, in_app_conversions) = "GOOGLE_PLAY"
      AND in_app_conversions = "FIREBASE"
      THEN TRUE
    ELSE
      FALSE
    END
    AS firebase_bid,
  CASE
    WHEN download_conversions = "GOOGLE_PLAY" AND in_app_conversions IN ("FIREBASE", "GOOGLE_PLAY")
      THEN FALSE
    WHEN
      ARRAY_LENGTH(SPLIT(IFNULL(download_conversions, in_app_conversions), ",")) = 1
      AND (
        in_app_conversions IS NULL
        OR in_app_conversions = download_conversions)
      THEN FALSE
    ELSE
      TRUE
    END
    AS mix_bid,
  budget_constrained_days_l,
  budget_constrained_days,
  low_bid_days,
  low_bid_days_l,
  ifnull(change_frequency_bp, TRUE) change_frequency_bp,
  segments_ad_networks,
  segments_ad_network_type segments_ad_network_type,
  installs,
  in_app_actions,
  metrics_clicks,
  metrics_impressions,
  metrics_conversions_value,
  metrics_conversions,
  metrics_cost
FROM
  `${datasetId}.base_snd_campaign_performance` base
LEFT JOIN
  (
    SELECT
      campaign_id,
      segments_date,
      STRING_AGG(
        DISTINCT
          IF(
            segments_conversion_action_category = "DOWNLOAD",
            segments_conversion_source,
            NULL))
        download_conversions,
      STRING_AGG(
        DISTINCT
          IF(
            segments_conversion_action_category != "DOWNLOAD",
            segments_conversion_source,
            NULL))
        in_app_conversions
    FROM
      `${datasetId}.app_snd_campaign_conv_report`
    GROUP BY
      1,
      2
  ) check
  USING (
    campaign_id,
    segments_date)
LEFT JOIN
  `${datasetId}.base_snd_campaigns` camp
  USING (
    campaign_id,
    segments_date)
LEFT JOIN
  bp_budget
  USING (
    campaign_id,
    segments_date)
LEFT JOIN
  (
    SELECT
      campaign_id,
      budget_constrained_days budget_constrained_days_l,
      low_bid_days low_bid_days_l
    FROM
      bp_budget
    INNER JOIN
      (
        SELECT
          campaign_id,
          MAX(segments_date) segments_date,
        FROM
          `${datasetId}.base_snd_campaign_performance`
        GROUP BY
          campaign_id
      )
      USING (
        campaign_id,
        segments_date)
  )
  USING (campaign_id)
LEFT JOIN
  `${datasetId}.app_snd_account_changes` change
  USING (
    campaign_id,
    segments_date)
WHERE
  base.advertising_channel_type = "MULTI_CHANNEL"