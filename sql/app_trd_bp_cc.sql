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
  campaign_app_campaign_setting_app_id,
  geo_target_constant_canonical_name geo_target_constant_canonical_name,
  customer_id,
  campaign_app_campaign_setting_app_store,
  campaign_name,
  campaign_id,
  customer_descriptive_name,
  aci_1,
  aci_2,
  aci_25,
  aci_3,
  total_camps,
  metrics_cost_7d,
  IFNULL(conversion_actions, "") conversion_actions
FROM
  (
    SELECT
      customer_id,
      campaign_app_campaign_setting_app_store,
      campaign_name,
      campaign_id,
      customer_descriptive_name,
      campaign_app_campaign_setting_app_id,
      geo_target_constant_canonical_name,
      SUM(aci_1) aci_1,
      SUM(aci_2) aci_2,
      SUM(aci_25) aci_25,
      SUM(aci_3) aci_3,
      MAX(campaigns) max_camps,
      SUM(campaigns) total_camps,
      ROUND(SUM(metrics_cost), 2) metrics_cost_7d
    FROM
      (
        SELECT
          r.customer_id,
          campaign_app_campaign_setting_app_store,
          r.campaign_name,
          r.campaign_id,
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
          SUM(metrics_cost) metrics_cost
        FROM
          `${datasetId}.base_snd_geo_perf_report` r
        INNER JOIN
          (
            SELECT
              campaign_id,
              campaign_name,
              country_name,
              campaign_app_campaign_setting_bidding_strategy_goal_type,
              customer_id,
              campaign_app_campaign_setting_app_id,
              customer_descriptive_name,
              campaign_app_campaign_setting_app_store
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
          ) camp
          ON
            camp.campaign_id = r.campaign_id
            AND (
              country_name IS NULL OR INSTR(country_name, geo_target_constant_canonical_name) != 0)
        INNER JOIN
          (
            SELECT
              campaign_id,
              MAX(segments_week) max_week
            FROM
              `${datasetId}.base_snd_geo_perf_report`
            GROUP BY
              1
          ) t
          ON
            r.campaign_id = t.campaign_id
            AND r.segments_week
              BETWEEN DATE_ADD(t.max_week, INTERVAL -14 day)
              AND DATE_ADD(t.max_week, INTERVAL -7 day)
        WHERE
          geo_target_constant_canonical_name IS NOT NULL
        GROUP BY
          customer_id,
          campaign_app_campaign_setting_app_store,
          campaign_name,
          campaign_id,
          customer_descriptive_name,
          campaign_app_campaign_setting_app_id,
          geo_target_constant_canonical_name,
          campaign_app_campaign_setting_bidding_strategy_goal_type
      )
    GROUP BY
      1,
      2,
      3,
      4,
      5,
      6,
      7
    HAVING max_camps > 1
  )
LEFT JOIN
  (
    SELECT
      r.campaign_id,
      STRING_AGG(DISTINCT segments_conversion_action_name, ", ") conversion_actions,
      SUM(metrics_conversions) metrics_conversions_14d
    FROM
      `${datasetId}.app_snd_campaign_conv_report` r
    INNER JOIN
      (
        SELECT
          campaign_id,
          MAX(segments_date) max_day
        FROM
          `${datasetId}.app_snd_campaign_conv_report`
        GROUP BY
          1
      ) t
      ON
        r.campaign_id = t.campaign_id
        AND r.segments_date
          BETWEEN DATE_ADD(t.max_day, INTERVAL -14 day)
          AND t.max_day
    WHERE
      metrics_conversions > 0
    GROUP BY
      1
  )
  USING (campaign_id)
WHERE
  metrics_cost_7d > 0