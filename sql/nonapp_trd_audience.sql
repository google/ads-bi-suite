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
  a.customer.descriptive_name AS Account,
  a.customer.id AS Customer_ID,
  a.campaign.name AS Campaign,
  a.campaign.id AS Campaign_ID,
  a.campaign.status AS Campaign_status,
  a.campaign.advertising_channel_type AS Campaign_type,
  a.campaign.advertising_channel_sub_type AS Campaign_sub_type,
  b.name AS Audience,
  b.type AS Audience_type,
  a.customer.currency_code AS Currency,
  CAST(a.segments.week AS DATE) AS Week,
  a.metrics.clicks AS Clicks,
  a.metrics.impressions AS Impressions,
  a.metrics.cost_micros / 1000000 AS Cost,
  a.metrics.conversions AS Conversions,
  a.metrics.conversions_value AS Conv_value,
  a.metrics.all_conversions AS All_conversions,
  a.metrics.all_conversions_value AS All_conv_value
FROM `${datasetId}.report_nonapp_campaign_perf_all_audience` a
LEFT JOIN
  (
    SELECT user_list.resource_name AS user_list, user_list.name, user_list.type
    FROM `${datasetId}.report_nonapp_user_lists`
  ) b
  ON a.ad_group_criterion.user_list.user_list = b.user_list
WHERE DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')