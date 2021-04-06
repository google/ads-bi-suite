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
CREATE TEMP FUNCTION
  getConversionSource(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "WEBPAGE",
    "ANALYTICS",
    "UPLOAD",
    "AD_CALL_METRICS",
    "WEBSITE_CALL_METRICS",
    "STORE_VISITS",
    "ANDROID_IN_APP",
    "IOS_IN_APP",
    "IOS_FIRST_OPEN",
    "APP_UNSPECIFIED",
    "ANDROID_FIRST_OPEN",
    "UPLOAD_CALLS",
    "FIREBASE",
    "CLICK_TO_CALL",
    "SALESFORCE",
    "STORE_SALES_CRM",
    "STORE_SALES_PAYMENT_NETWORK",
    "GOOGLE_PLAY",
    "THIRD_PARTY_APP_ANALYTICS",
    "GOOGLE_ATTRIBUTION",
    "STORE_SALES_DIRECT_UPLOAD",
    "STORE_SALES"][
  OFFSET
    (status)]);
SELECT
  camp.*,
  conv.segments_conversion_action_name,
  event_name,
  includeInConversion,
  countType,
  conversion_actions,
  conversion_source,
  conversion_action_resource,
  metrics_conversions_value,
  metrics_conversions,
  metrics_all_conversions_value,
  metrics_all_conversions,
  metrics_impressions,
  metrics_clicks,
  metrics_cost,
  installs,
  in_app_actions
FROM (
  SELECT
    campaign.id campaign_id,
    segments.date segments_date,
    segments.conversionActionName segments_conversion_action_name,
    segments.conversionAction conversion_action_resource,
    getConversionActionCategory(segments.conversionActionCategory) segments_conversion_action_category,
    getConversionSource(segments.externalConversionSource) conversion_source,
    SUM(metrics.conversionsValue) metrics_conversions_value,
    SUM(metrics.conversions) metrics_conversions,
    SUM(metrics.allConversionsValue) metrics_all_conversions_value,
    SUM(metrics.allConversions) metrics_all_conversions
  FROM
    `${datasetId}.report_base_campaign_conversion`
  WHERE
    (DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
        '${partitionDay}')
      OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day))
    AND (metrics.conversions > 0
      OR metrics.allConversions > 0)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6) conv
LEFT JOIN (
  SELECT
    campaign_id,
    segments_date,
    COUNT(DISTINCT segments_conversionAction) conversion_actions,
    SUM(installs) installs,
    SUM(in_app_actions) in_app_actions
  FROM (
    SELECT
      campaign.id campaign_id,
      segments.date segments_date,
      segments.conversionAction segments_conversionAction,
    IF
      (getConversionActionCategory(segments.conversionActionCategory) = "DOWNLOAD",
        SUM(metrics.conversions),
        0) installs,
    IF
      (getConversionActionCategory(segments.conversionActionCategory) != "DOWNLOAD",
        SUM(metrics.conversions),
        0) in_app_actions
    FROM
      `${datasetId}.report_base_campaign_conversion`
    WHERE
      ( DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
          '${partitionDay}')
        OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day) )
      AND (metrics.conversions > 0
        OR metrics.allConversions > 0)
    GROUP BY
      campaign.id,
      segments.date,
      segments.conversionActionCategory,
      segments.conversionAction)
  GROUP BY
    1,
    2)
USING
  (campaign_id,
    segments_date)
LEFT JOIN (
  SELECT
    DISTINCT conversionAction.resourceName conversion_action_resource,
  IF
    (conversionAction.thirdPartyAppAnalyticsSettings.eventName IS NULL,
      conversionAction.firebaseSettings.eventName,
      conversionAction.thirdPartyAppAnalyticsSettings.eventName) event_name,
    conversionAction.includeInConversionsMetric includeInConversion,
    conversionAction.countingType countType
  FROM
    `${datasetId}.report_app_conversion_action` ) event
USING
  (conversion_action_resource)
INNER JOIN (
  SELECT
    campaign.id campaign_id,
    segments.date segments_date,
    SUM(metrics.clicks) metrics_clicks,
    SUM(metrics.impressions) metrics_impressions,
    ROUND(SUM(metrics.costMicros)/1e6,2) metrics_cost
  FROM
    `${datasetId}.report_base_campaign_performance`
  WHERE
    DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
      '${partitionDay}' )
    OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day)
  GROUP BY
    1,
    2) perf
USING
  (campaign_id,
    segments_date)
INNER JOIN
  `${datasetId}.app_snd_campaigns` camp
USING
  (campaign_id,
    segments_date)