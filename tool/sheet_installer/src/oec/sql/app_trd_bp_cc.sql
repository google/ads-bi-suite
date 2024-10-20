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
  camp AS (
    SELECT
      campaign_id,
      campaign_name,
      country_name,
      campaign_app_campaign_setting_bidding_strategy_goal_type,
      customer_id,
      campaign_app_campaign_setting_app_id,
      customer_descriptive_name,
      campaign_app_campaign_setting_app_store,
      segments_date,
      campaign_budget_amount,
      currency
    FROM
      `${datasetId}.base_snd_campaigns`
    INNER JOIN
      (
        SELECT
          campaign_id,
          MAX(segments_date) segments_date
        FROM
          `${datasetId}.base_snd_campaigns`
        WHERE
          campaign_app_campaign_setting_bidding_strategy_goal_type != "AC For Engagement"
          AND campaign_status = "ENABLED"
          AND advertising_channel_type = "MULTI_CHANNEL"
        GROUP BY
          1
      )
      USING (
        campaign_id,
        segments_date)
  )
SELECT DISTINCT
  base.campaign_app_campaign_setting_app_id,
  campaign_app_campaign_setting_bidding_strategy_goal_type,
  geo_target_constant_canonical_name geo_target_constant_canonical_name,
  base.customer_id,
  base.campaign_app_campaign_setting_app_store,
  base.campaign_name,
  base.campaign_id,
  base.customer_descriptive_name,
  currency,
  aci_1,
  aci_2,
  aci_25,
  aci_3,
  total_camps,
  campaign_budget_amount,
  IFNULL(conversion_actions, "") conversion_actions
FROM
  (
    SELECT
      country_name,
      customer_id,
      customer_descriptive_name,
      camp.campaign_id,
      camp.campaign_name,
      camp.campaign_app_campaign_setting_bidding_strategy_goal_type,
      campaign_app_campaign_setting_app_id,
      campaign_budget_amount,
      campaign_app_campaign_setting_app_store,
      conversion_actions,
      currency
    FROM
      camp
    LEFT JOIN
      (
        SELECT
          r.campaign_id,
          STRING_AGG(DISTINCT segments_conversion_action_name, ", ") conversion_actions
        FROM
          `${datasetId}.app_snd_campaign_conv_report` r
        INNER JOIN
          (
            SELECT
              campaign_id,
              MAX(segments_date) max_date
            FROM
              `${datasetId}.app_snd_campaign_conv_report`
            GROUP BY
              1
          ) conv
          ON
            r.campaign_id = conv.campaign_id
            AND r.segments_date BETWEEN DATE_ADD(conv.max_date, INTERVAL -7 day) AND segments_date
        WHERE
          segments_conversion_action_name IS NOT NULL
        GROUP BY 1
      )
      USING (campaign_id)
  ) base
INNER JOIN
  (
    SELECT
      customer_id,
      customer_descriptive_name,
      campaign_app_campaign_setting_app_id,
      geo_target_constant_canonical_name,
      SUM(aci_1) aci_1,
      SUM(aci_2) aci_2,
      SUM(aci_25) aci_25,
      SUM(aci_3) aci_3,
      MAX(campaigns) max_camps,
      SUM(campaigns) total_camps
    FROM
      (
        SELECT
          r.customer_id,
          r.customer_descriptive_name,
          campaign_app_campaign_setting_app_id,
          geo_target_constant_canonical_name,
          IF(
            campaign_app_campaign_setting_bidding_strategy_goal_type = "AC for Install 1.0",
            COUNT(DISTINCT r.campaign_id),
            0) AS aci_1,
          IF(
            campaign_app_campaign_setting_bidding_strategy_goal_type = "AC for Install 2.0",
            COUNT(DISTINCT r.campaign_id),
            0) AS aci_2,
          IF(
            campaign_app_campaign_setting_bidding_strategy_goal_type = "AC for Action",
            COUNT(DISTINCT r.campaign_id),
            0) AS aci_25,
          IF(
            campaign_app_campaign_setting_bidding_strategy_goal_type = "AC for Value",
            COUNT(DISTINCT r.campaign_id),
            0) AS aci_3,
          COUNT(DISTINCT r.campaign_id) campaigns,
        FROM
          `${datasetId}.base_snd_geo_perf_report` r
        INNER JOIN
          camp
          ON
            camp.campaign_id = r.campaign_id
            AND (
              country_name IS NULL
              OR INSTR(country_name, geo_target_constant_canonical_name) != 0)
        WHERE geo_target_constant_canonical_name IS NOT NULL
        GROUP BY
          customer_id,
          customer_descriptive_name,
          campaign_app_campaign_setting_app_id,
          geo_target_constant_canonical_name,
          campaign_app_campaign_setting_bidding_strategy_goal_type,
          segments_date
      )
    GROUP BY
      1,
      2,
      3,
      4
    HAVING
      max_camps > 2
  ) cc
  ON
    base.customer_id = cc.customer_id
    AND base.campaign_app_campaign_setting_app_id = cc.campaign_app_campaign_setting_app_id
    AND (
      base.country_name IS NULL
      OR INSTR(base.country_name, cc.geo_target_constant_canonical_name) != 0)