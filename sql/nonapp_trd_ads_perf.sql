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
  a.customer.currency_code AS Currency,
  a.campaign.name AS Campaign,
  a.campaign.id AS Campaign_ID,
  a.campaign.status AS Campaign_status,
  a.campaign.advertising_channel_type AS Campaign_type,
  a.campaign.advertising_channel_sub_type AS Campaign_sub_type,
  a.ad_group.name AS Ad_group_name,
  a.ad_group_ad.ad.id AS Ad_id,
  a.ad_group_ad.ad.name AS Ad_name,
  a.ad_group_ad.ad.type AS Ad_type,
  a.ad_group_ad.status AS Ad_status,
  a.ad_group_ad.ad.expanded_text_ad.headline_part1 AS Expanded_text_ad_headline_part1,
  a.ad_group_ad.ad.expanded_text_ad.headline_part2 AS Expanded_text_ad_headline_part2,
  a.ad_group_ad.ad.expanded_text_ad.path1 AS Expanded_text_ad_path1,
  a.ad_group_ad.ad.responsive_search_ad.path1 AS Responsive_search_ad_path1,
  a.ad_group_ad.ad.responsive_display_ad.long_headline.text AS Responsive_display_ad_long_headline,
  a.ad_group_ad.ad.text_ad.headline AS Text_ad_headline,
  CAST(a.segments.week AS DATE) AS Week,
  a.metrics.clicks AS Clicks,
  a.metrics.impressions AS Impressions,
  a.metrics.cost_micros / 1000000 AS Cost,
  a.metrics.conversions AS Conversions,
  a.metrics.conversions_value AS Conv_value,
  a.metrics.all_conversions AS All_conversions,
  a.metrics.all_conversions_value AS All_conv_value,
FROM `${datasetId}.report_nonapp_campaign_perf_all_ad` a
WHERE DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')