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

SELECT
  camp.*,
  metrics.optimization_score_uplift optimization_score_uplift,
  metrics.optimization_score_url optimization_score_url
FROM
  `${datasetId}.report_app_recommendations` r
INNER JOIN
  (
    SELECT
      campaign.id campaign_id,
      MAX(_PARTITIONTIME) PARTITIONTIME
    FROM
      `${datasetId}.report_app_recommendations`
    WHERE
      metrics.optimization_score_uplift IS NOT NULL
    GROUP BY
      1
  ) rec
  ON
    r.campaign.id = rec.campaign_id
    AND r._PARTITIONTIME = rec.PARTITIONTIME
INNER JOIN
  (
    SELECT
      customer_descriptive_name,
      customer_id,
      currency,
      campaign_id,
      advertising_channel_sub_type,
      advertising_channel_type,
      campaign_name,
      campaign_status,
      campaign_bidding_strategy_type,
      campaign_app_campaign_setting_app_id,
      campaign_app_campaign_setting_app_store,
      campaign_app_campaign_setting_bidding_strategy_goal_type,
      language_code,
      country_code,
      campaign_budget_amount,
      campaign_target_roas_target_roas,
      campaign_target_cpa_target_cpa,
      campaign_optimization_score,
    FROM
      `${datasetId}.base_snd_campaigns`
    INNER JOIN
      (
        SELECT
          campaign_id,
          MAX(segments_date) segments_date
        FROM
          `${datasetId}.base_snd_campaigns`
        GROUP BY
          1
      )
      USING (
        campaign_id,
        segments_date)
  ) camp
  USING (campaign_id)