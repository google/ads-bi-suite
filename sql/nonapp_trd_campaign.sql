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
 
select
 distinct c.customer.id as Customer_ID,
 account.customer.descriptiveName as Account,

 c.campaign.id as Campaign_ID,
 c.campaign.name as Campaign,
 c.customer.currencyCode as Currency,
 getCampaignStatus(c.campaign.status) as Campaign_status,
 p.budget as Budget,
 p.spend_7d as Cost_7d,
 p.budget_utilization as Budget_utilization,
 c.segments.date as Date,
 cast(c.segments.week as DATE) as Week,
  getAdNetwork(c.segments.adNetworkType) as Ad_network_type,
 getDeviceType(c.segments.device) as Device,
 
 w.week1_cost as Week1_cost,
 w.week2_cost as Week2_cost,
 w.cost_wow as Cost_WOW,
 c.metrics.clicks as Clicks,
 w.week1_clicks as Week1_clicks,
 w.week2_clicks as Week2_clicks,
 w.clicks_wow as Clicks_WOW,
 w.week1_conversion_value as Week1_conversion_value,
 w.week2_conversion_value as Week2_conversion_value,
 w.week1_all_conversion_value as Week1_all_conversion_value,
 w.week2_all_conversion_value as Week2_all_conversion_value,
 w.week1_conversions as Week1_conversions,
 w.week2_conversions as Week2_conversions,
 conversions_wow as Conversions_WOW,
 
 getCampaignType(c.campaign.advertisingChannelType) as Campaign_type,
 getCampaignSubType(c.campaign.advertisingChannelSubType) as Campaign_sub_type,
 a.all_ads,
 a.disapproved_ads,
 a.underreview_ads,
 
 c.metrics.costMicros/1000000 as Cost,
 c.metrics.impressions as Impressions,
 c.metrics.conversions as Conversions,
 c.metrics.allConversions as All_conversions,
 c.metrics.conversionsValue as Conv_value,
 c.metrics.allConversionsValue as All_conv_value,
 c.metrics.videoViews as Video_view,
 
 conv.Add_to_cart,
 conv.Purchase,
 conv.Lead,
 conv.Signup,
 conv.Check_out
 
from `${datasetId}.report_base_campaign_performance` c
left join `${datasetId}.nonapp_snd_campaign_perf_pacing` p
on c.campaign.id = p.id
left join `${datasetId}.nonapp_snd_campaign_ads_approval` a
on a.campaign_id = c.campaign.id
left join `${datasetId}.nonapp_snd_campaign_perf_wow` w
on w.Campaign_id = c.campaign.id
left join `${datasetId}.report_base_account_performance` account
on account.customer.id = c.customer.id

 
Left join (
SELECT
   campaign.id,
   segments.date segments_date,
   segments.adNetworkType segments_ad_network_type,
   SUM(
   IF
     (getConversionActionCategory(segments.conversionActionCategory) = "ADD_TO_CART",
       metrics.conversions,
       0)) Add_to_cart,
   SUM(
   IF
     (getConversionActionCategory(segments.conversionActionCategory) = "BEGIN_CHECKOUT",
       metrics.conversions,
       0)) Check_out,
   SUM(
   IF
     (getConversionActionCategory(segments.conversionActionCategory) = "PURCHASE",
       metrics.conversions,
       0)) Purchase,
   SUM(
   IF
     (getConversionActionCategory(segments.conversionActionCategory) = "SIGNUP",
       metrics.conversions,
       0)) Signup,
   SUM(
   IF
     (getConversionActionCategory(segments.conversionActionCategory) IN ( "SUBMIT_LEAD_FORM", 'LEAD','IMPORTED_LEAD'),
       metrics.conversions,
       0)) Lead,
 
 FROM
   `${datasetId}.report_base_campaign_conversion`
 WHERE
   (DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}'))
        AND metrics.conversions > 0
 GROUP BY
   1,
   2,
   3) conv
 
ON
 c.campaign.id = conv.id
and   c.segments.date = conv.segments_date
and   c.segments.adNetworkType = conv.segments_ad_network_type
 
 
where date(c._partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
