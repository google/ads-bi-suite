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
  campaign_budget_changes AS (
    SELECT
      camp.campaign.id AS campaign_id,
      change.change_event.change_date_time AS change_date_time,
      change.change_event.change_resource_type AS change_resource_type,
      camp.campaign_budget.amount_micros AS campaign_budget_amount,
    FROM
      `{datasetId}.report_base_account_change_event` change
    LEFT JOIN
      `{datasetId}.report_base_campaigns` camp
      ON
        change.campaign.id = camp.campaign.id
        AND DATE(change_event.change_date_time) = DATE(camp._PARTITIONTIME)
    WHERE
      DATE(change._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      AND change.change_event.change_resource_type IN ("CAMPAIGN_BUDGET")
      AND change.change_event.change_date_time
        > DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY)
      AND camp.campaign.id IS NOT NULL
    GROUP BY
      1,
      2,
      3,
      4
    ORDER BY
      camp.campaign.id,
      change.change_event.change_date_time
  ),
  campaign_tartget_cpa_changes AS (
    SELECT
      camp.campaign.id AS campaign_id,
      change.change_event.change_date_time AS change_date_time,
      change.change_event.change_resource_type AS change_resource_type,
      camp.campaign.target_cpa.target_cpa_micros AS campaign_target_cpa_target_cpa
    FROM
      `{datasetId}.report_base_account_change_event` change
    LEFT JOIN
      `{datasetId}.report_base_campaigns` camp
      ON
        change.campaign.id = camp.campaign.id
        AND DATE(change_event.change_date_time) = DATE(camp._PARTITIONTIME)
    WHERE
      DATE(change._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
      AND change.change_event.change_resource_type IN ("CAMPAIGN")
      AND change.change_event.change_date_time
        > DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY)
      AND camp.campaign.id IS NOT NULL
    GROUP BY
      1,
      2,
      3,
      4
    ORDER BY
      camp.campaign.id,
      change.change_event.change_date_time
  ),
  change_diffs AS (
    SELECT
      *,
      CASE
        WHEN prev_budget_amount IS NULL THEN 0
        WHEN prev_budget_amount = 0 THEN 1  # handle divided by 0 case
        ELSE
          (prev_budget_amount - campaign_budget_amount) / prev_budget_amount
        END
        AS change_diff
    FROM
      (
        SELECT
          *,
          LAG(campaign_budget_amount)
            OVER (PARTITION BY campaign_id ORDER BY change_date_time) AS prev_budget_amount
        FROM
          campaign_budget_changes
      )
    UNION ALL
    # Calculate the change difference for campaign_target_cpa
    SELECT
      *,
      CASE
        WHEN prev_budget_amount IS NULL THEN 0
        WHEN prev_budget_amount = 0 THEN 1  # handle divided by 0 case
        ELSE
          (prev_budget_amount - campaign_target_cpa_target_cpa) / prev_budget_amount
        END
        AS change_diff
    FROM
      (
        SELECT
          *,
          LAG(campaign_target_cpa_target_cpa)
            OVER (PARTITION BY campaign_id ORDER BY change_date_time) AS prev_budget_amount
        FROM
          campaign_tartget_cpa_changes
      )
  ),
  new_campaigns AS (
    SELECT
      perf.campaign.id AS campaign_id,
      MIN(perf.segments.date) AS campaign_start_date
    FROM
      `{datasetId}.report_base_campaign_performance` perf
    LEFT JOIN
      `{datasetId}.report_base_campaign_conversion` conv
      ON
        conv.campaign.id = perf.campaign.id
        AND conv.segments.date = perf.segments.date
    WHERE
      DATE(perf._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
        AND DATE(conv._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
        AND perf.metrics.impressions > 0
      OR conv.metrics.conversions < 100
    GROUP BY
      1
    HAVING MIN(perf.segments.date) > DATE_SUB(MIN(perf._PARTITIONDATE), INTERVAL 30 DAY)
  )
SELECT
  *,
  (four_less_changes AND new_campaign_no_change) AS change_frequency_bp
FROM
  (
    SELECT
      campaign_id,
      MAX(DATE(change_date_time)) AS segments_date,
      COUNTIF(ABS(change_diff) > 0.2) AS large_changes,
      COUNTIF(ABS(change_diff) > 0.01) AS changes,
      new_campaigns.campaign_start_date,
      IF(COUNTIF(ABS(change_diff) > 0.2) >= 3, FALSE, TRUE) AS four_less_changes,
      IF(
        DATE_DIFF(MAX(DATE(change_date_time)), new_campaigns.campaign_start_date, DAY) <= 7
          AND COUNTIF(ABS(change_diff) > 0.01) > 0,
        FALSE,
        TRUE) AS new_campaign_no_change
    FROM
      change_diffs
    LEFT JOIN
      new_campaigns
      USING (campaign_id)
    GROUP BY
      campaign_id,
      new_campaigns.campaign_start_date
    ORDER BY
      campaign_id
  )