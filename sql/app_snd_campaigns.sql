CREATE TEMP FUNCTION
  getCampaignStatus(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "ENABLED",
    "PAUSED",
    "REMOVED"][
  OFFSET
    (status)]);
CREATE TEMP FUNCTION
  getAdvertisingChannelType(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "SEARCH",
    "DISPLAY",
    "SHOPPING",
    "HOTEL",
    "VIDEO",
    "MULTI_CHANNEL",
    "LOCAL",
    "SMART"][
  OFFSET
    (status)]);
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
  getAdGroupStatus(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "ENABLED",
    "PAUSED",
    "REMOVED"][
  OFFSET
    (status)]);
CREATE TEMP FUNCTION
  getBiddingStrategyType(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST",
    "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST",
    "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST",
    "OPTIMIZE_RETURN_ON_ADVERTISING_SPEND"][
  OFFSET
    (status)]);
CREATE TEMP FUNCTION
  getAppStore(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "APPLE_APP_STORE",
    "GOOGLE_APP_STORE"][
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
CREATE TEMP FUNCTION
  getAdvertisingChannelSubType(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "SEARCH_MOBILE_APP",
    "DISPLAY_MOBILE_APP",
    "SEARCH_EXPRESS",
    "DISPLAY_EXPRESS",
    "SHOPPING_SMART_ADS",
    "DISPLAY_GMAIL_AD",
    "DISPLAY_SMART_CAMPAIGN",
    "VIDEO_OUTSTREAM",
    "VIDEO_ACTION",
    "VIDEO_NON_SKIPPABLE",
    "APP_CAMPAIGN",
    "APP_CAMPAIGN_FOR_ENGAGEMENT",
    "LOCAL_CAMPAIGN",
    "SHOPPING_COMPARISON_LISTING_ADS",
    "SMART_CAMPAIGN",
    "VIDEO_SEQUENCE"][
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
WITH
  camp AS(
  SELECT
    segments_date,
    camp.*
  FROM (
    SELECT
      DISTINCT segments.date segments_date
    FROM
      `${datasetId}.report_base_campaign_performance` perf
    INNER JOIN (
      SELECT
        DATE_ADD(DATE(MIN(_PARTITIONTIME)), INTERVAL -1 day) launch_date
      FROM
        `${datasetId}.report_base_campaigns`)
    ON
      segments.date < launch_date
    WHERE
      DATE(perf._partitionTime) = PARSE_DATE('%Y%m%d',
        '${partitionDay}' )
      OR perf.segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day))
  LEFT JOIN (
    SELECT
      *
    FROM
      `${datasetId}.report_base_campaigns`
    WHERE
      _PARTITIONTIME IN (
      SELECT
        MIN(_PARTITIONTIME)
      FROM
        `${datasetId}.report_base_campaigns`) ) camp
  ON
    1=1
  UNION ALL (
    SELECT
      DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -1 day) AS segments_date,
      *
    FROM
      `${datasetId}.report_base_campaigns` )
  UNION ALL (
    SELECT
      DATE_ADD(PARSE_DATE('%Y%m%d',
          '${partitionDay}'), INTERVAL -1 day) AS segments_date,
      *
    FROM
      `${datasetId}.report_base_campaigns`
    WHERE
      _PARTITIONTIME IN (
      SELECT
        MAX(_PARTITIONTIME)
      FROM
        `${datasetId}.report_base_campaigns`) ) )
SELECT
  DISTINCT customer.currencyCode customer_currency_code,
  DATE_ADD(DATE(camp.segments_date), INTERVAL (2-EXTRACT(DAYOFWEEK
      FROM
        camp.segments_date)) day) AS segments_week,
  camp.segments_date segments_date,
  campaign.id campaign_id,
  getAdvertisingChannelSubType( campaign.advertisingChannelSubType ) advertising_channel_sub_type,
  campaign.name campaign_name,
  customer.descriptivename customer_descriptive_name,
  customer.id customer_id,
  ifnull(adg.ad_groups,
    0) ad_groups,
  adg.metrics_cost_total,
  campaign.appCampaignSetting.appid campaign_app_campaign_setting_app_id,
  ROUND(campaignBudget.amountMicros/1e6,2) campaign_budget_amount,
IF
  (campaign.appCampaignSetting.appStore IS NOT NULL,
    getAppStore(campaign.appCampaignSetting.appStore),
    NULL) campaign_app_campaign_setting_app_store,
IF
  (campaign.appCampaignSetting.biddingStrategyGoalType IS NOT NULL,
    CASE
      WHEN getAdvertisingChannelSubType(campaign.advertisingChannelSubType) = "APP_CAMPAIGN_FOR_ENGAGEMENT" THEN "ACe"
      WHEN getBiddingStrategyType(campaign.appCampaignSetting.biddingStrategyGoalType) = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST" THEN "AC for Install 1.0"
      WHEN getBiddingStrategyType(campaign.appCampaignSetting.biddingStrategyGoalType) = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST" THEN "AC for Action"
      WHEN getBiddingStrategyType(campaign.appCampaignSetting.biddingStrategyGoalType) = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_INSTALL_COST" THEN "AC for Install 2.0"
      WHEN getBiddingStrategyType(campaign.appCampaignSetting.biddingStrategyGoalType) = "OPTIMIZE_RETURN_ON_ADVERTISING_SPEND" THEN "AC for Value"
    ELSE
    "Max Conversion"
  END
    ,
    NULL) AS campaign_app_campaign_setting_bidding_strategy_goal_type,
  getCampaignStatus(campaign.status) campaign_status,
  IFNULL(campaign.targetRoas.targetRoas,
    0) campaign_target_roas_target_roas,
IF
  (campaign.targetCpa.targetCpaMicros IS NOT NULL,
    ROUND(campaign.targetCpa.targetCpaMicros/1e6,2),
    0) campaign_target_cpa_target_cpa,
  CASE
    WHEN getBiddingStrategyType(campaign.appCampaignSetting.biddingStrategyGoalType) = "OPTIMIZE_INSTALLS_TARGET_INSTALL_COST" AND campaignBudget.amountMicros/campaign.targetCpa.targetCpaMicros < 50 THEN "Budget < 50x CPI"
    WHEN getBiddingStrategyType(campaign.appCampaignSetting.biddingStrategyGoalType) = "OPTIMIZE_IN_APP_CONVERSIONS_TARGET_CONVERSION_COST"
  AND campaignBudget.amountMicros/campaign.targetCpa.targetCpaMicros < 10 THEN "Budget < 10x CPA"
  ELSE
  "PASS"
END
  AS budget_excellence_reason,
  CASE
    WHEN download_conversions = "FIREBASE" AND in_app_conversions IN ("", "GOOGLE_PLAY", "FIREBASE") THEN TRUE
    WHEN download_conversions = "GOOGLE_PLAY"
  AND in_app_conversions = "FIREBASE" THEN TRUE
  ELSE
  FALSE
END
  AS firebase_bid,
  CASE
    WHEN ARRAY_LENGTH(SPLIT(download_conversions,",")) > 1 OR ARRAY_LENGTH(SPLIT(in_app_conversions,",")) > 1 THEN TRUE
    WHEN download_conversions = "GOOGLE_PLAY"
  OR in_app_conversions = "GOOGLE_PLAY" THEN FALSE
    WHEN REPLACE(in_app_conversions, download_conversions,"") = "" THEN FALSE
  ELSE
  TRUE
END
  AS mix_bid
FROM
  camp
LEFT JOIN (
  SELECT
    campaign.id campaign_id,
    segments.date segments_date,
    COUNT(DISTINCT adGroup.id ) ad_groups,
    ROUND(SUM(metrics.costMicros)/1e6,2) metrics_cost_total
  FROM
    `${datasetId}.report_app_ad_group_perf`
  WHERE
    getAdGroupStatus(adGroup.status) = "ENABLED"
    AND (DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
        '${partitionDay}' )
      OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day))
    AND getAdGroupStatus(adGroup.status) = "ENABLED"
  GROUP BY
    1,
    2 ) adg
ON
  campaign.id = adg.campaign_id
  AND adg.segments_date = camp.segments_date
LEFT JOIN (
  SELECT
    campaign_id,
    segments_date,
    STRING_AGG(download_conversions,"") download_conversions,
    STRING_AGG(in_app_conversions,"") in_app_conversions
  FROM (
    SELECT
      campaign.id campaign_id,
      segments.date segments_date,
    IF
      (getConversionActionCategory(segments.conversionActionCategory) = "DOWNLOAD",
        STRING_AGG(DISTINCT getConversionSource(segments.externalConversionSource)),
        "") download_conversions,
    IF
      (getConversionActionCategory(segments.conversionActionCategory) != "DOWNLOAD",
        STRING_AGG(DISTINCT getConversionSource(segments.externalConversionSource)),
        "") in_app_conversions
    FROM
      `${datasetId}.report_base_campaign_conversion`
    WHERE
      (DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
          '${partitionDay}')
        OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day))
      AND metrics.conversions > 0
    GROUP BY
      campaign.id,
      segments.date,
      segments.conversionActionCategory)
  GROUP BY
    1,
    2 ) check
ON
  campaign.id = check.campaign_id
  AND check.segments_date = camp.segments_date
WHERE
  getAdvertisingChannelType(campaign.advertisingChannelType) = "MULTI_CHANNEL"