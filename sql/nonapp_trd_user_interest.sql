CREATE TEMP FUNCTION getCampaignStatus(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','ENABLED','PAUSED','REMOVED'][OFFSET(status)]);

CREATE TEMP FUNCTION getCampaignType(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','SEARCH','DISPLAY','SHOPPING','HOTEL','VIDEO','MULTI_CHANNEL','LOCAL','SMART'][OFFSET(status)]);

CREATE TEMP FUNCTION getCampaignSubType(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','SEARCH_MOBILE_APP','DISPLAY_MOBILE_APP','SEARCH_EXPRESS','DISPLAY_EXPRESS','SHOPPING_SMART_ADS','DISPLAY_GMAIL_AD','DISPLAY_SMART_CAMPAIGN','VIDEO_OUTSTREAM','VIDEO_ACTION','VIDEO_NON_SKIPPABLE','APP_CAMPAIGN','APP_CAMPAIGN_FOR_ENGAGEMENT','LOCAL_CAMPAIGN','SHOPPING_COMPARISON_LISTING_ADS','SMART_CAMPAIGN','VIDEO_SEQUENCE'][OFFSET(status)]);

CREATE TEMP FUNCTION getUserInterestTaxonomyType(status INT64)
AS (['UNSPECIFIED','UNKNOWN','AFFINITY','IN_MARKET','MOBILE_APP_INSTALL_USER','VERTICAL_GEO','NEW_SMART_PHONE_USER'][OFFSET(status)]);


select distinct
  a.customer.descriptiveName as Account,
  a.customer.id as Customer_ID,
  a.campaign.name as Campaign,
  a.campaign.id as Campaign_ID,
  getCampaignStatus(a.campaign.status) as Campaign_status,
  getCampaignType(a.campaign.advertisingChannelType) as Campaign_type,
  getCampaignSubType(a.campaign.advertisingChannelSubType) as Campaign_sub_type,
  b.userInterestName as User_interest,
  getUserInterestTaxonomyType(b.userInterestTaxonomyType) as User_interest_taxonomy,
  a.customer.currencyCode as Currency,
  cast(a.segments.week as DATE) as Week,
  a.metrics.clicks as Clicks,
  a.metrics.impressions as Impressions,
  a.metrics.costMicros/1000000 as Cost,
  a.metrics.conversions as Conversions,
  a.metrics.conversionsValue as Conv_value,
  a.metrics.allConversions as All_conversions,
  a.metrics.allConversionsValue as All_conv_value
from `${datasetId}.report_nonapp_campaign_perf_user_interest` a
left join
  (select userInterest.UserInterestId as userInterestId, userInterest.name as userInterestName, userInterest.taxonomyType as userInterestTaxonomyType
   from `${datasetId}.report_nonapp_user_interest` ) b
on REGEXP_EXTRACT(a.adGroupCriterion.userInterest.userInterestCategory, r"[^\/]+$") = cast(b.userInterestId as STRING)

where DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
