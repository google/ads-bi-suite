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
  a.extension_feed_item.sitelink_feed_item.line2 AS Sitelink2,
  a.extension_feed_item.sitelink_feed_item.line1 AS Sitelink1,
  a.campaign.advertising_channel_type AS Campaign_type,
  a.campaign.advertising_channel_sub_type AS Campaign_sub_type,
  a.metrics.conversions AS Conversions,
  a.customer.currency_code AS Currency,
  a.campaign.id AS Campaign_id,
  a.metrics.clicks AS Clicks,
  a.metrics.conversions_value AS Conv_value,
  a.metrics.impressions AS Impressions,
  a.extension_feed_item.sitelink_feed_item.link_text AS Link_text,
  a.campaign.name AS Campaign_name,
  a.customer.descriptive_name AS Account,
  a.campaign.status AS Campaign_status,
  a.customer.id AS Customer_ID,
  CAST(a.segments.week AS DATE) AS Week,
  a.metrics.cost_micros / 1000000 AS Cost,
  a.metrics.all_conversions AS All_converdsions,
  a.metrics.all_conversions_value AS All_conv_value
FROM `${datasetId}.report_nonapp_campaign_perf_search_extensions` a
WHERE date(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')