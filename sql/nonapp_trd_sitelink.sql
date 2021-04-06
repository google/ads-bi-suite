CREATE TEMP FUNCTION getCampaignStatus(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','ENABLED','PAUSED','REMOVED'][OFFSET(status)]);

CREATE TEMP FUNCTION getCampaignType(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','SEARCH','DISPLAY','SHOPPING','HOTEL','VIDEO','MULTI_CHANNEL','LOCAL','SMART'][OFFSET(status)]);

CREATE TEMP FUNCTION getCampaignSubType(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','SEARCH_MOBILE_APP','DISPLAY_MOBILE_APP','SEARCH_EXPRESS','DISPLAY_EXPRESS','SHOPPING_SMART_ADS','DISPLAY_GMAIL_AD','DISPLAY_SMART_CAMPAIGN','VIDEO_OUTSTREAM','VIDEO_ACTION','VIDEO_NON_SKIPPABLE','APP_CAMPAIGN','APP_CAMPAIGN_FOR_ENGAGEMENT','LOCAL_CAMPAIGN','SHOPPING_COMPARISON_LISTING_ADS','SMART_CAMPAIGN','VIDEO_SEQUENCE'][OFFSET(status)]);

select
distinct
a.extensionFeedItem.sitelinkFeedItem.line2 as Sitelink2,
a.extensionFeedItem.sitelinkFeedItem.line1 as Sitelink1,
getCampaignType(a.campaign.advertisingChannelType) as Campaign_type,
getCampaignSubType(a.campaign.advertisingChannelSubType) as Campaign_sub_type,
a.metrics.conversions as Conversions,
a.customer.currencyCode as Currency,
a.campaign.id as Campaign_id,
a.metrics.clicks as Clicks,
#a.metrics_ctr as CTR,
a.metrics.conversionsValue as Conv_value,
a.metrics.impressions as Impressions,
a.extensionFeedItem.sitelinkFeedItem.linkText as Link_text,
a.campaign.name as Campaign_name,
a.customer.descriptiveName as Account,
getCampaignStatus(a.campaign.status) as Campaign_status,
a.customer.id as Customer_ID,
cast(a.segments.week as DATE) as Week,
a.metrics.costMicros/1000000 as Cost,
a.metrics.allConversions as All_converdsions,
a.metrics.allConversionsValue as All_conv_value
from
`${datasetId}.report_nonapp_campaign_perf_search_extensions` a
where

date(a._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')