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
  camp.campaign_app_campaign_setting_bidding_strategy_goal_type,
  camp.campaign_app_campaign_setting_app_id,
  camp.country_code,
  camp.country_name,
  p.*
FROM
  (
    SELECT
      campaign.id campaign_id,
      campaign.name campaign_name,
      campaign.status campaign_status,
      customer.id customer_id,
      customer.currency_code customer_currency_code,
      customer.descriptive_name customer_descriptive_name,
      campaign.advertising_channel_sub_type campaign_advertising_channel_sub_type,
      group_placement_view.display_name display_name,
      group_placement_view.placement_type placement_type,
      group_placement_view.target_url target_url,
      metrics.impressions metrics_impressions,
      metrics.clicks metrics_clicks,
      ROUND(metrics.cost_micros / 1e6, 2) metrics_cost,
      metrics.conversions_value metrics_conversions_value,
      metrics.conversions metrics_conversions,
      metrics.all_conversions_value metrics_all_conversions_value,
      metrics.all_conversions metrics_all_conversions,
      ROW_NUMBER() OVER (PARTITION BY campaign.id ORDER BY metrics.impressions DESC) rank
    FROM
      `${datasetId}.report_base_detail_placement_view`
    WHERE
      campaign.advertising_channel_type = "MULTI_CHANNEL"
      AND metrics.cost_micros > 0
      AND DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
  ) p
INNER JOIN
  `${datasetId}.base_snd_campaigns` camp
  USING (campaign_id)
WHERE rank < 100