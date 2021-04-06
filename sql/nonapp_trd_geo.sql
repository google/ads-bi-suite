CREATE TEMP FUNCTION getCampaignStatus(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','ENABLED','PAUSED','REMOVED'][OFFSET(status)]);

CREATE TEMP FUNCTION getCampaignType(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','SEARCH','DISPLAY','SHOPPING','HOTEL','VIDEO','MULTI_CHANNEL','LOCAL','SMART'][OFFSET(status)]);

CREATE TEMP FUNCTION getCampaignSubType(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','SEARCH_MOBILE_APP','DISPLAY_MOBILE_APP','SEARCH_EXPRESS','DISPLAY_EXPRESS','SHOPPING_SMART_ADS','DISPLAY_GMAIL_AD','DISPLAY_SMART_CAMPAIGN','VIDEO_OUTSTREAM','VIDEO_ACTION','VIDEO_NON_SKIPPABLE','APP_CAMPAIGN','APP_CAMPAIGN_FOR_ENGAGEMENT','LOCAL_CAMPAIGN','SHOPPING_COMPARISON_LISTING_ADS','SMART_CAMPAIGN','VIDEO_SEQUENCE'][OFFSET(status)]);

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

CREATE TEMP FUNCTION getDeviceType(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','MOBILE','TABLET','DESKTOP','CONNECTED_TV','OTHER'][OFFSET(status)]);

select distinct
  a.customer.descriptiveName as Account,
  a.customer.id as Customer_ID,
  a.campaign.name as Campaign,
  a.campaign.id as Campaign_ID,
  getCampaignStatus(a.campaign.status) as Campaign_status,
  getCampaignType(a.campaign.advertisingChannelType) as Campaign_type,
  getCampaignSubType(a.campaign.advertisingChannelSubType) as Campaign_sub_type,
  a.customer.currencyCode as Currency,
  cast(a.segments.week as DATE) as Week,
  t.name as Country_Territory,
  getAdNetwork(segments.adNetworkType) as Ad_network_type,
  getDeviceType(a.segments.device) as Device,
  a.metrics.clicks as Clicks,
  a.metrics.impressions as Impressions,
  a.metrics.costMicros/1000000 as Cost,
  a.metrics.conversions as Conversions,
  a.metrics.conversionsValue as Conv_value,
  a.metrics.allConversions as All_conversions,
  a.metrics.allConversionsValue as All_conv_value
from `${datasetId}.report_base_geographic_view` a
left join
  (select geoTargetConstant.id, geoTargetConstant.name from `${datasetId}.report_base_geo_target_constant`
   group by 1, 2) t
on a.geographicView.countryCriterionId = t.id

where DATE(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
#group by 1,2,3,4,5,6,7,8,9,10,11