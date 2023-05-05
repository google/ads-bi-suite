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
  cpp.date_string,
  cpp.universal_account_id,
  cpp.source_account_id,
  cpp.parent_id,
  cpp.division_id,
  cpp.company_id,
  cpp.third_party_parent_name,
  cpp.third_party_parent_id,
  cpp.optiscore_search_net_score_legacy_q422_numerator,
  cpp.optiscore_search_net_score_legacy_q422_denominator,
  cpp.optiscore_search_net_score_legacy_q422_value,
  cpp.bfm_search_adjusted_optiscore_numerator,
  cpp.bfm_search_adjusted_optiscore_denominator,
  cpp.bfm_search_adjusted_optiscore_value,
  cpp.bfm_action_trueview_for_action_denominator,
  cpp.bfm_action_trueview_for_action_numerator,
  cpp.bfm_action_trueview_for_action_value,
  cpp.bfm_apps_firebase_bidding_v2_denominator,
  cpp.bfm_apps_firebase_bidding_v2_numerator,
  cpp.bfm_apps_firebase_bidding_v2_value,
  cpp.bfm_measurement_enhanced_conversions_denominator,
  cpp.bfm_measurement_enhanced_conversions_numerator,
  cpp.bfm_measurement_enhanced_conversions_value,
  cpp.bfm_measurement_google_analytics_4_source_of_truth_denominator,
  cpp.bfm_measurement_google_analytics_4_source_of_truth_numerator,
  cpp.bfm_measurement_google_analytics_4_source_of_truth_value,
  cpp.bfm_search_coverage_denominator,
  cpp.bfm_search_coverage_numerator,
  cpp.bfm_search_coverage_value,
  cpp.bfm_search_value_based_bidding_denominator,
  cpp.bfm_search_value_based_bidding_numerator,
  cpp.bfm_search_value_based_bidding_value,
  cpp.optiscore_apps_net_score_denominator,
  cpp.optiscore_apps_net_score_numerator,
  cpp.optiscore_apps_net_score_value,
  cpp.agency_name,
  cpp.customer_id,
  cpp.mcc_cid,
  l.account,
  l.currency,
  l.Billing_profile_id,
  l.Billing_profile,
  c.rate_usd,
  c.rate_aud,
  c.rate_sgd,
  s.cost_search_28d,
  s.cost_shopping_28d,
  s.cost_ac_28d,
  s.cost_vac_28d,
  s.cost_display_28d,
  s.cost_nonac_28d,
  s.cost_video_28d,
  s.cost_28d,
  l.label,
  SUM(
    CASE
      WHEN c.Campaign_type = 'SEARCH' THEN c.Cost
      ELSE
        NULL
      END)
    * c.rate_usd cost_search,
  SUM(
    CASE
      WHEN c.Campaign_type = 'SHOPPING' THEN c.Cost
      ELSE
        NULL
      END)
    * c.rate_usd cost_shopping,
  SUM(
    CASE
      WHEN c.Campaign_sub_type = 'VIDEO_ACTION' THEN c.Cost
      ELSE
        NULL
      END)
    * c.rate_usd cost_vac,
  SUM(
    CASE
      WHEN c.Campaign_type = 'MULTI_CHANNEL' THEN c.Cost
      ELSE
        NULL
      END)
    * c.rate_usd cost_ac,
  SUM(
    CASE
      WHEN c.Campaign_type = 'VIDEO' THEN c.Cost
      ELSE
        NULL
      END)
    * c.rate_usd cost_video,
  SUM(
    CASE
      WHEN c.Campaign_type = 'DISPLAY' THEN c.Cost
      ELSE
        NULL
      END)
    * c.rate_usd cost_display,
  SUM(
    CASE
      WHEN c.Campaign_type != 'MULTI_CHANNEL' THEN c.Cost
      ELSE
        NULL
      END)
    * c.rate_usd cost_nonac,
  SUM(c.cost) * c.rate_usd cost
FROM
  `${datasetId}.cpp_bfm_metrics_mcc_*` cpp
LEFT JOIN
  `${datasetId}.nonapp_trd_campaign_with_label` c
  ON
    cpp.customer_id = c.customer_id
    AND CAST(cpp.date_string AS date) = c.Date
LEFT JOIN
  (
    SELECT
      customer_id,
      rate_usd,
      SUM(
        CASE
          WHEN c.Campaign_type = 'SEARCH' THEN c.Cost
          ELSE
            NULL
          END)
        * rate_usd cost_search_28d,
      SUM(
        CASE
          WHEN c.Campaign_type = 'SHOPPING' THEN c.Cost
          ELSE
            NULL
          END)
        * rate_usd cost_shopping_28d,
      SUM(
        CASE
          WHEN c.Campaign_sub_type = 'VIDEO_ACTION' THEN c.Cost
          ELSE
            NULL
          END)
        * rate_usd cost_vac_28d,
      SUM(
        CASE
          WHEN c.Campaign_type = 'MULTI_CHANNEL' THEN c.Cost
          ELSE
            NULL
          END)
        * rate_usd cost_ac_28d,
      SUM(
        CASE
          WHEN c.Campaign_type = 'VIDEO' THEN c.Cost
          ELSE
            NULL
          END)
        * rate_usd cost_video_28d,
      SUM(
        CASE
          WHEN c.Campaign_type = 'DISPLAY' THEN c.Cost
          ELSE
            NULL
          END)
        * rate_usd cost_display_28d,
      SUM(
        CASE
          WHEN c.Campaign_type != 'MULTI_CHANNEL' THEN c.Cost
          ELSE
            NULL
          END)
        * rate_usd cost_nonac_28d,
      SUM(c.cost) * rate_usd cost_28d
    FROM
      `${datasetId}.nonapp_trd_campaign_with_label` c
    WHERE
      date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 day)
    GROUP BY
      1,
      2
  ) s
  ON
    cpp.customer_id = s.customer_id
LEFT JOIN
  (
    SELECT DISTINCT
      account,
      currency,
      Billing_profile_id,
      Billing_profile,
      customer_id,
      label
    FROM
      `${datasetId}.nonapp_trd_campaign_with_label` c
    WHERE c.Date = DATE_SUB(CURRENT_DATE(), INTERVAL 7 day)
  ) l
  ON
    cpp.customer_id = l.customer_id
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7,
  8,
  9,
  10,
  11,
  12,
  13,
  14,
  15,
  16,
  17,
  18,
  19,
  20,
  21,
  22,
  23,
  24,
  25,
  26,
  27,
  28,
  29,
  30,
  31,
  32,
  33,
  34,
  35,
  36,
  37,
  38,
  39,
  40,
  41,
  42,
  43,
  44,
  45,
  46,
  47,
  48,
  49,
  50,
  51,
  52,
  53,
  54
