SELECT
  campaign_id,
  campaign_name,
  campaign_app_campaign_setting_app_id,
  campaign_app_campaign_setting_bidding_strategy_goal_type,
  customer_id,
  customer_descriptive_name,
  conv.segments_week,
  segments_conversion_action_name,
  conv.geographic_view_country_criterion_id,
  geo_target_constant_canonical_name,
  metrics_conversions_value,
  metrics_conversions,
  metrics_all_conversions_value,
  metrics_all_conversions
FROM (
  SELECT
    campaign.id campaign_id,
    segments.week segments_week,
    segments.conversionActionName segments_conversion_action_name,
    geographicView.countryCriterionId geographic_view_country_criterion_id,
    SUM(metrics.conversionsValue) metrics_conversions_value,
    SUM(metrics.conversions) metrics_conversions,
    SUM(metrics.allConversionsValue) metrics_all_conversions_value,
    SUM(metrics.allConversions) metrics_all_conversions
  FROM
    `${datasetId}.report_app_geo_conversion`
  WHERE
    DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
      '${partitionDay}')
    OR segments.week < DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -(EXTRACT(DAYOFWEEK
        FROM
          segments.week)+30) day)
  GROUP BY
    1,
    2,
    3,
    4) conv
INNER JOIN (
  SELECT
    DISTINCT geoTargetConstant.id geographic_view_country_criterion_id,
    geoTargetConstant.canonicalName geo_target_constant_canonical_name
  FROM
    `${datasetId}.report_base_geo_target_constant`) c
USING
  (geographic_view_country_criterion_id)
INNER JOIN
  `${datasetId}.app_snd_campaigns` camp
USING
  (campaign_id,
    segments_week)