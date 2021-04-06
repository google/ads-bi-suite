CREATE TEMP FUNCTION getCampaignStatus(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','ENABLED','PAUSED','REMOVED'][OFFSET(status)]);

CREATE TEMP FUNCTION getAdGroupAdStatus(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','ENABLED','PAUSED','REMOVED'][OFFSET(status)]);

CREATE TEMP FUNCTION getCampaignType(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','SEARCH','DISPLAY','SHOPPING','HOTEL','VIDEO','MULTI_CHANNEL','LOCAL','SMART'][OFFSET(status)]);

CREATE TEMP FUNCTION getCampaignSubType(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','SEARCH_MOBILE_APP','DISPLAY_MOBILE_APP','SEARCH_EXPRESS','DISPLAY_EXPRESS','SHOPPING_SMART_ADS','DISPLAY_GMAIL_AD','DISPLAY_SMART_CAMPAIGN','VIDEO_OUTSTREAM','VIDEO_ACTION','VIDEO_NON_SKIPPABLE','APP_CAMPAIGN','APP_CAMPAIGN_FOR_ENGAGEMENT','LOCAL_CAMPAIGN','SHOPPING_COMPARISON_LISTING_ADS','SMART_CAMPAIGN','VIDEO_SEQUENCE'][OFFSET(status)]);

CREATE TEMP FUNCTION getAdType(TypeCode INT64)
AS
(CASE TypeCode
  WHEN 0 THEN 'UNSPECIFIED'
  WHEN 1 THEN 'UNKNOWN'
  WHEN 2 THEN 'TEXT_AD'
  WHEN 3 THEN 'EXPANDED_TEXT_AD'
  WHEN 6 THEN 'CALL_ONLY_AD'
  WHEN 7 THEN 'EXPANDED_DYNAMIC_SEARCH_AD'
  WHEN 8 THEN 'HOTEL_AD'
  WHEN 9 THEN 'SHOPPING_SMART_AD'
  WHEN 10 THEN 'SHOPPING_PRODUCT_AD'
  WHEN 12 THEN 'VIDEO_AD'
  WHEN 13 THEN 'GMAIL_AD'
  WHEN 14 THEN 'IMAGE_AD'
  WHEN 15 THEN 'RESPONSIVE_SEARCH_AD'
  WHEN 16 THEN 'LEGACY_RESPONSIVE_DISPLAY_AD'
  WHEN 17 THEN 'APP_AD'
  WHEN 18 THEN 'LEGACY_APP_INSTALL_AD'
  WHEN 19 THEN 'RESPONSIVE_DISPLAY_AD'
  WHEN 20 THEN 'LOCAL_AD'
  WHEN 21 THEN 'HTML5_UPLOAD_AD'
  WHEN 22 THEN 'DYNAMIC_HTML5_AD'
  WHEN 23 THEN 'APP_ENGAGEMENT_AD'
  WHEN 24 THEN 'SHOPPING_COMPARISON_LISTING_AD'
  WHEN 25 THEN 'VIDEO_BUMPER_AD'
  WHEN 26 THEN 'VIDEO_NON_SKIPPABLE_IN_STREAM_AD'
  WHEN 27 THEN 'VIDEO_OUTSTREAM_AD'
  WHEN 28 THEN 'VIDEO_TRUEVIEW_DISCOVERY_AD'
  WHEN 29 THEN 'VIDEO_TRUEVIEW_IN_STREAM_AD'
  WHEN 30 THEN 'VIDEO_RESPONSIVE_AD'
 END);

select distinct
  a.customer.descriptiveName as Account,
  a.customer.id as Customer_ID,
  a.customer.currencyCode as Currency,
  a.campaign.name as Campaign,
  a.campaign.id as Campaign_ID,
  getCampaignStatus(a.campaign.status) as Campaign_status,
  getCampaignType(a.campaign.advertisingChannelType) as Campaign_type,
  getCampaignSubType(a.campaign.advertisingChannelSubType) as Campaign_sub_type,
  a.adGroup.name as Ad_group_name,
  a.adGroupAd.ad.id as Ad_id,
  a.adGroupAd.ad.name as Ad_name,
  getAdType(a.adGroupAd.ad.type) as Ad_type,
  getAdGroupAdStatus(a.adGroupAd.status) as Ad_status,

  a.adGroupAd.ad.expandedTextAd.headlinePart1 as Expanded_text_ad_headline_part1,
  a.adGroupAd.ad.expandedTextAd.headlinePart2 as Expanded_text_ad_headline_part2,
  a.adGroupAd.ad.expandedTextAd.path1 as Expanded_text_ad_path1,
  a.adGroupAd.ad.responsiveSearchAd.path1 as Responsive_search_ad_path1,
  a.adGroupAd.ad.responsiveDisplayAd.longHeadline.text as Responsive_display_ad_long_headline,
  a.adGroupAd.ad.textAd.headline as Text_ad_headline,

  cast(a.segments.week as DATE) as Week,
  a.metrics.clicks as Clicks,
  a.metrics.impressions as Impressions,
  a.metrics.costMicros/1000000 as Cost,
  a.metrics.conversions as Conversions,
  a.metrics.conversionsValue as Conv_value,
  a.metrics.allConversions as All_conversions,
  a.metrics.allConversionsValue as All_conv_value,

from `${datasetId}.report_nonapp_campaign_perf_all_ad` a

where DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
