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
  latest_records AS (
    SELECT
      id,
      MAX(_PARTITIONTIME) _PARTITIONTIME
    FROM
      `${datasetId}.report_youtube_channels`
    GROUP BY
      id
  )
SELECT DISTINCT
  id,
  statistics.videoCount videoCount,
  brandingSettings.channel.keywords keywords,
  IF(snippet.country IS NOT NULL, snippet.country, brandingSettings.channel.country) country,
  snippet.defaultLanguage defaultLanguage,
  IF(statistics.hiddenSubscriberCount IS FALSE, statistics.subscriberCount, NULL) subscriberCount,
  statistics.viewCount viewCount,
  IF(
    snippet.localized.title IS NOT NULL,
    snippet.localized.title,
    IF(
      snippet.title IS NOT NULL,
      snippet.title,
      brandingSettings.channel.title))
    title,
  IF(
    snippet.localized.description IS NOT NULL,
    snippet.localized.description,
    brandingSettings.channel.description)
    description,
  snippet.thumbnails.high.url thumbnail,
  categories
FROM
  `${datasetId}.report_youtube_channels` c
LEFT JOIN
  (
    SELECT
      id,
      STRING_AGG(DISTINCT REPLACE(category, "https://en.wikipedia.org/wiki/", ""), " , ") categories
    FROM
      `${datasetId}.report_youtube_channels` c
    CROSS JOIN
      c.topicDetails.topicCategories category
    INNER JOIN
      latest_records
      USING (
        id,
        _PARTITIONTIME)
    GROUP BY
      id
  )
  USING (id)
INNER JOIN
  latest_records
  USING (id, _PARTITIONTIME)