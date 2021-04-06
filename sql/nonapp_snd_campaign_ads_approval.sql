CREATE TEMP FUNCTION getPolicyApprovalStatus(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','DISAPPROVED','APPROVED_LIMITED','APPROVED','AREA_OF_INTEREST_ONLY'][OFFSET(status)]);

CREATE TEMP FUNCTION getPolicyReviewStatus(status INT64)
  AS (['UNSPECIFIED','UNKNOWN','REVIEW_IN_PROGRESS','REVIEWED','UNDER_APPEAL','ELIGIBLE_MAY_SERVE'][OFFSET(status)]);

select
  customer.id as customer_id,
  campaign.id as campaign_id,
  campaign.name as campaign_name,
  count(if(getPolicyApprovalStatus(adGroupAd.policySummary.approvalStatus) LIKE 'DISAPPROVED', adGroupAd.policySummary.approvalStatus, null)) as disapproved_ads,
  count(if(getPolicyReviewStatus(adGroupAd.policySummary.reviewStatus) LIKE 'REVIEW_IN_PROGRESS', adGroupAd.policySummary.approvalStatus, null)) as underreview_ads,
  count(adGroupAd.ad.id) as all_ads
from `${datasetId}.report_base_campaign_ads_approval`

where date(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
group by
  customer_id,
  campaign_id,
  campaign_name