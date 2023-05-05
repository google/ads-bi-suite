-- Copyright 2023 Google LLC
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

-- Backfill missing campaign metadata based on performance data
WITH
  camp AS (
    SELECT
      DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -1 day) AS segments_date,
      *
    FROM
      `${datasetId}.report_base_campaigns`
    UNION ALL
    (
      SELECT
        perf_date segments_date,
        camp.*
      FROM
        `${datasetId}.report_base_campaigns` camp
      INNER JOIN
        (
          SELECT
            f.campaign_id,
            perf_date,
            MIN(camp.segments_date) fix
          FROM
            (
              -- If f.camp_segments_date is null, the campaign metadata of that day is missing
              SELECT
                perf.segments_date AS perf_date,
                perf.campaign_id,
                camp.segments_date camp_segments_date
              FROM
                (
                  SELECT DISTINCT
                    campaign.id campaign_id,
                    segments.date segments_date
                  FROM
                    `${datasetId}.report_base_campaign_performance`
                  ORDER BY
                    segments.date DESC
                ) perf
              LEFT JOIN
                (
                  SELECT DISTINCT
                    campaign.id campaign_id,
                    DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -1 day) AS segments_date
                  FROM
                    `${datasetId}.report_base_campaigns`
                ) camp
                USING (campaign_id, segments_date)
              ORDER BY
                perf.segments_date ASC
            ) f
          INNER JOIN
            (
              SELECT DISTINCT
                campaign.id campaign_id,
                DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -1 day) AS segments_date
              FROM
                `${datasetId}.report_base_campaigns`
            ) camp
            ON
              f.perf_date < camp.segments_date
              AND camp.campaign_id = f.campaign_id
          WHERE
            f.camp_segments_date IS NULL
          GROUP BY
            1,
            2
          ORDER BY
            perf_date DESC
        ) fix
        ON
          camp.campaign.id = fix.campaign_id
          AND DATE_ADD(DATE(camp._PARTITIONTIME), INTERVAL -1 day) = fix.fix
      ORDER BY
        segments_date DESC
    )
  )

-- Generate campaign metadata
SELECT DISTINCT
  DATE_ADD(DATE(camp.segments_date), INTERVAL(2 - EXTRACT(DAYOFWEEK FROM camp.segments_date)) day)
    AS segments_week,
  customer.descriptive_name customer_descriptive_name,
  customer.id customer_id,
  customer.currency_code currency,
  camp.segments_date segments_date,
  campaign.id campaign_id,
  campaign.advertising_channel_sub_type advertising_channel_sub_type,
  campaign.advertising_channel_type advertising_channel_type,
  campaign.name campaign_name,
  campaign.status campaign_status,
  campaign.bidding_strategy_type campaign_bidding_strategy_type,
  campaign.app_campaign_setting.app_id campaign_app_campaign_setting_app_id,
  IFNULL(campaign.app_campaign_setting.app_store, NULL) campaign_app_campaign_setting_app_store,
  IF(
    campaign.advertising_channel_type = "MULTI_CHANNEL",
    CASE
      WHEN campaign.advertising_channel_sub_type = "APP_CAMPAIGN_FOR_ENGAGEMENT"
        THEN "AC For Engagement"
      WHEN
        campaign.advertising_channel_sub_type = "UNKNOWN"
        AND campaign.bidding_strategy_type = "TARGET_CPA"
        AND campaign.advertising_channel_sub_type = "UNKNOWN"
        THEN "AC for Pre-registration"
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
      WHEN
        campaign.advertising_channel_sub_type = "UNKNOWN"
        AND campaign.bidding_strategy_type = "MAXIMIZE_CONVERSIONS"
        THEN "Max Conversion"
      ELSE
        "UNKNOWN"
      END,
    NULL) AS campaign_app_campaign_setting_bidding_strategy_goal_type,
  IFNULL(language_name, "") language_name,
  IFNULL(language_code, "") language_code,
  IFNULL(country_code, "") country_code,
  IFNULL(country_name, "") country_name,
  IFNULL(campaign.url_expansion_opt_out, false) as url_expansion_opt_out,
  IFNULL(campaign_budget.has_recommended_budget, false) as limited_by_budget,
  campaign.shopping_setting.merchant_id as merchant_id,
  ROUND(AVG(campaign_budget.amount_micros) / 1e6, 2) campaign_budget_amount,
  AVG(campaign.target_roas.target_roas) campaign_target_roas_target_roas,
  ROUND(AVG(campaign.target_cpa.target_cpa_micros) / 1e6, 2) campaign_target_cpa_target_cpa,
  ROUND(AVG(campaign.optimization_score), 2) campaign_optimization_score
FROM
  camp
LEFT JOIN
  (
    -- Aggregate target country and language codes by campaign
    SELECT
      campaign_id,
      STRING_AGG(
        DISTINCT l.language_constant.name
        ORDER BY
          l.language_constant.name ASC)
        language_name,
      STRING_AGG(
        DISTINCT l.language_constant.code
        ORDER BY
          l.language_constant.code ASC)
        language_code,
      STRING_AGG(
        DISTINCT g.geo_target_constant.country_code
        ORDER BY
          g.geo_target_constant.country_code ASC)
        country_code,
      STRING_AGG(
        DISTINCT g.geo_target_constant.name
        ORDER BY
          g.geo_target_constant.name ASC)
        country_name
    FROM
      (
        SELECT DISTINCT
          campaign.id campaign_id,
          campaign_criterion.LANGUAGE.language_constant language_constant,
          campaign_criterion.location.geo_target_constant geo_target_constant
        FROM
          `${datasetId}.report_base_campaign_criterion`
        WHERE
          (
            campaign_criterion.LANGUAGE.language_constant IS NOT NULL
            OR campaign_criterion.location.geo_target_constant IS NOT NULL)
          AND campaign_criterion.negative IS NOT TRUE
      ) raw
    LEFT JOIN
      `${datasetId}.report_base_language_constant` l
      ON
        l.language_constant.resource_name = raw.language_constant
    LEFT JOIN
      `${datasetId}.report_base_geo_target_constant` g
      ON
        g.geo_target_constant.resource_name = raw.geo_target_constant
    GROUP BY 1
  ) cc
  ON
    cc.campaign_id = camp.campaign.id
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21