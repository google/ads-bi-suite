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
SELECT
  camp.*,
  segments_ad_networks,
  getAdNetwork(network.segments_ad_network_type) segments_ad_network_type,
  ifnull(installs,
    0) installs,
  ifnull(in_app_actions,
    0) in_app_actions,
  metrics_clicks,
  metrics_conversions_value,
  metrics_impressions,
  metrics_conversions,
  metrics_cost
FROM (
  SELECT
    segments.date segments_date,
    campaign.id campaign_id,
    segments.adNetworkType segments_ad_network_type,
    SUM(metrics.clicks) metrics_clicks,
    SUM(metrics.conversionsValue) metrics_conversions_value,
    SUM(metrics.impressions) metrics_impressions,
    ROUND(SUM(metrics.costMicros)/1e6,2) metrics_cost,
    SUM(metrics.conversions) metrics_conversions
  FROM
    `${datasetId}.report_base_campaign_performance`
  WHERE
    DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
      '${partitionDay}')
    OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day)
  GROUP BY
    1,
    2,
    3) network
LEFT JOIN (
  SELECT
    segments.date segments_date,
    campaign.id campaign_id,
    COUNT(DISTINCT segments.adNetworkType) segments_ad_networks
  FROM
    `${datasetId}.report_base_campaign_performance`
  WHERE
    DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
      '${partitionDay}')
    OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day)
  GROUP BY
    1,
    2)
USING
  (segments_date,
    campaign_id)
LEFT JOIN (
  SELECT
    campaign.id campaign_id,
    segments.date segments_date,
    segments.adNetworkType segments_ad_network_type,
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
    `${datasetId}.report_base_campaign_conversion`
  WHERE
    (DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
        '${partitionDay}')
      OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day) )
    AND metrics.conversions > 0
  GROUP BY
    1,
    2,
    3) conv
USING
  (campaign_id,
    segments_date,
    segments_ad_network_type)
INNER JOIN
  `${datasetId}.app_snd_campaigns` camp
USING
  (campaign_id,
    segments_date)