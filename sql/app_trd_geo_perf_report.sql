CREATE TEMP FUNCTION
  getAdNetwork(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "SEARCH",
    "SEARCH_PARTNERS",
    "CONTENT",
    "YOUTUBE_SEARCH",
    "YOUTUBE_WATCH",
    "MIXED"][
  OFFSET
    (status)]);
CREATE TEMP FUNCTION
  getConversionActionCategory(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "DEFAULT",
    "PAGE_VIEW",
    "PURCHASE",
    "SIGNUP",
    "LEAD",
    "DOWNLOAD",
    "ADD_TO_CART",
    "BEGIN_CHECKOUT",
    "SUBSCRIBE_PAID",
    "PHONE_CALL_LEAD",
    "IMPORTED_LEAD",
    "SUBMIT_LEAD_FORM",
    "BOOK_APPOINTMENT",
    "REQUEST_QUOTE",
    "GET_DIRECTIONS",
    "OUTBOUND_CLICK",
    "CONTACT",
    "ENGAGEMENT",
    "STORE_VISIT",
    "STORE_SALE"][
  OFFSET
    (status)]);
SELECT distinct
  customer_currency_code,
  campaign_name,
  customer_descriptive_name,
  campaign_status,
  customer_id,
  campaign_app_campaign_setting_app_id,
  campaign_app_campaign_setting_app_store,
  campaign_app_campaign_setting_bidding_strategy_goal_type,
  advertising_channel_sub_type,
  geo.campaign_id,
  geo.segments_week,
  firebase_bid,
  ad_groups,
  geo_target_constant_canonical_name,
  ifnull(conv.installs,
    0) installs,
  ifnull(conv.in_app_actions,
    0) in_app_actions,
  getAdNetwork(segments_ad_network_type) segments_ad_network_type,
  metrics_clicks,
  metrics_conversions_value,
  metrics_impressions,
  metrics_conversions,
  metrics_cost
FROM (
  SELECT
    campaign.id campaign_id,
    segments.week segments_week,
    segments.adNetworkType segments_ad_network_type,
    geographicView.countryCriterionId geographicView_countryCriterionId,
    SUM(metrics.clicks) metrics_clicks,
    SUM(metrics.conversionsValue) metrics_conversions_value,
    SUM(metrics.impressions) metrics_impressions,
    ROUND(SUM(metrics.costMicros)/1e6,2) metrics_cost,
    SUM(metrics.conversions) metrics_conversions
  FROM
    `${datasetId}.report_base_geographic_view`
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
    4) geo
INNER JOIN (
  SELECT
    DISTINCT geoTargetConstant.id geographicView_countryCriterionId,
    geoTargetConstant.canonicalName geo_target_constant_canonical_name
  FROM
    `${datasetId}.report_base_geo_target_constant`) c
USING
  (geographicView_countryCriterionId)
LEFT JOIN (
  SELECT
    campaign.id campaign_id,
    segments.week segments_week,
    segments.adNetworkType segments_ad_network_type,
    geographicView.countryCriterionId geographicView_countryCriterionId,
    SUM(
    IF
      (getConversionActionCategory(segments.conversionActionCategory) = "DOWNLOAD",
        metrics.conversions,
        0)) installs,
    SUM(
    IF
      (getConversionActionCategory(segments.conversionActionCategory) != "DOWNLOAD",
        metrics.conversions,
        0)) in_app_actions
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
    4 ) conv
USING
  (campaign_id,
    segments_week,
    geographicView_countryCriterionId,
    segments_ad_network_type)
INNER JOIN
  `${datasetId}.app_snd_campaigns` camp
USING
  (campaign_id,
    segments_week)
where camp.segments_date = camp.segments_week