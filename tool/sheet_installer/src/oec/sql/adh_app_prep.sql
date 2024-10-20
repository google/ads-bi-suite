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
  DISTINCT lego.campaign_id,
  lego.app_id,
  lego.conversion_id,
FROM (
  SELECT
    DISTINCT camp.campaign.id campaign_id,
    camp.campaign.app_campaign_setting.app_id app_id,
    SPLIT( segments.conversion_action ,"/")[
  OFFSET
    (3)] conversion_id,
    MAX(conv.segments.date) segments_date
  FROM
    `${legoDatasetId}.report_base_campaign_conversion` conv
  LEFT JOIN
    `${legoDatasetId}.report_base_campaigns` camp
  ON
    conv.campaign.id = camp.campaign.id
  WHERE
    segments.conversion_action_category = "DOWNLOAD"
    AND metrics.conversions > 0
    AND camp.campaign.app_campaign_setting.app_id IS NOT NULL
  GROUP BY
    1,
    2,
    3) lego
INNER JOIN (
  SELECT
    campaign.id campaign_id,
    MAX(segments.date) segments_date
  FROM
    `${legoDatasetId}.report_base_campaign_conversion`
  WHERE
    segments.conversion_action_category = "DOWNLOAD"
    AND metrics.conversions > 0
  GROUP BY
    1)
USING
  (campaign_id,
    segments_date)