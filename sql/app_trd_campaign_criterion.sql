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

WITH
  cc AS (
    SELECT segments_date, c.*
    FROM
      (
        SELECT DISTINCT segments.date segments_date
        FROM `${datasetId}.report_app_ad_group` adg
        INNER JOIN
          (
            SELECT DATE_ADD(DATE(MIN(_PARTITIONTIME)), INTERVAL -1 day) launch_date
            FROM `${datasetId}.report_app_campaign_criterion`
          )
          ON segments.date < launch_date
        WHERE
          DATE(adg._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
          OR adg.segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day)
      )
    LEFT JOIN
      (
        SELECT DISTINCT
          campaign.id campaign_id,
          campaign_criterion.LANGUAGE.language_constant language_constant,
          campaign_criterion.location.geo_target_constant geo_target_constant
        FROM `${datasetId}.report_app_campaign_criterion` c
        WHERE
          campaign_criterion.negative = FALSE
          AND _PARTITIONTIME IN (
            SELECT MIN(_PARTITIONTIME) FROM `${datasetId}.report_app_campaign_criterion`
          )
      ) c
      ON 1 = 1
    UNION ALL
    (
      SELECT DISTINCT
        DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -1 day) AS segments_date,
        campaign.id campaign_id,
        campaign_criterion.LANGUAGE.language_constant language_constant,
        campaign_criterion.location.geo_target_constant geo_target_constant
      FROM `${datasetId}.report_app_campaign_criterion`
      WHERE campaign_criterion.negative = FALSE
    )
  )
SELECT
  camp.*,
  l.language_constant.name language_name,
  l.language_constant.code language_code,
  g.geo_target_constant.canonical_name geo_target_constant_canonical_name,
  g.geo_target_constant.country_code geo_target_constant_country_code
FROM `${datasetId}.app_snd_campaigns` camp
LEFT JOIN (SELECT campaign_id, language_constant, geo_target_constant, segments_date FROM cc) c
  USING (campaign_id, segments_date)
LEFT JOIN `${datasetId}.report_base_language_constant` l
  ON l.language_constant.resource_name = c.language_constant
LEFT JOIN `${datasetId}.report_base_geo_target_constant` g
  ON g.geo_target_constant.resource_name = c.geo_target_constant
WHERE campaign_status = "ENABLED"