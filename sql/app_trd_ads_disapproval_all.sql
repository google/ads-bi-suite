CREATE TEMP FUNCTION
  getAdType(status INT64) AS ([ "UNSPECIFIED",
    --0
    "UNKNOWN",
    --1
    "TEXT_AD",
    --2
    "EXPANDED_TEXT_AD",
    --3
    "",
    "",
    "CALL_ONLY_AD",
    --6
    "EXPANDED_DYNAMIC_SEARCH_AD",
    "HOTEL_AD",
    "SHOPPING_SMART_AD",
    "SHOPPING_PRODUCT_AD",
    "",
    "VIDEO_AD",
    "GMAIL_AD",
    "IMAGE_AD",
    "RESPONSIVE_SEARCH_AD",
    "LEGACY_RESPONSIVE_DISPLAY_AD",
    "APP_AD",
    --17
    "LEGACY_APP_INSTALL_AD",
    "RESPONSIVE_DISPLAY_AD",
    "LOCAL_AD",
    "HTML5_UPLOAD_AD",
    "DYNAMIC_HTML5_AD",
    "APP_ENGAGEMENT_AD",
    --23
    "SHOPPING_COMPARISON_LISTING_AD",
    "VIDEO_BUMPER_AD",
    "VIDEO_NON_SKIPPABLE_IN_STREAM_AD",
    "VIDEO_OUTSTREAM_AD",
    "VIDEO_TRUEVIEW_DISCOVERY_AD",
    "VIDEO_TRUEVIEW_IN_STREAM_AD",
    "VIDEO_RESPONSIVE_AD" ][
  OFFSET
    (status)]);
CREATE TEMP FUNCTION
  getAssetFieldType(status INT64) AS (['UNSPECIFIED',
    'UNKNOWN',
    'HEADLINE',
    'DESCRIPTION',
    'MANDATORY_AD_TEXT',
    'MARKETING_IMAGE',
    'MEDIA_BUNDLE',
    'YOUTUBE_VIDEO'][
  OFFSET
    (status)]);
CREATE TEMP FUNCTION
  getApprovalStatus(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "DISAPPROVED",
    "APPROVED_LIMITED",
    "APPROVED",
    "AREA_OF_INTEREST_ONLY"][
  OFFSET
    (status)]);
CREATE TEMP FUNCTION
  getReviewStatus(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "REVIEW_IN_PROGRESS",
    "REVIEWED",
    "UNDER_APPEAL",
    "ELIGIBLE_MAY_SERVE"][
  OFFSET
    (status)]);
SELECT
  asset.ad_group_id,
  asset.asset_approval_status,
  asset.asset_review_status,
  asset.asset_id,
  asset.asset_type,
  ag.campaign_id,
  ag.customer_descriptive_name,
  ag.customer_id,
  ag.campaign_name,
  ag.ad_group_ad_ad_id,
  ag.date,
  ag.adgroup_approval_status,
  ag.adgroup_review_status
FROM (
  SELECT
    s.adGroup.id ad_group_id,
    DATE(s._partitionTime) date,
    CASE
      WHEN getApprovalStatus(s.adGroupAdAssetView.policySummary.approvalStatus) IS NULL THEN 'N/A'
    ELSE
    getApprovalStatus(s.adGroupAdAssetView.policySummary.approvalStatus)
  END
    asset_approval_status,
    getReviewStatus(s.adGroupAdAssetView.policySummary.reviewStatus ) asset_review_status,
    s.asset.id asset_id,
    getAssetFieldType(s.adGroupAdAssetView.fieldType ) asset_type,
  FROM
    `${datasetId}.report_app_disapprovals_ad_group_ad_asset_view` s
  WHERE
    DATE(s._partitionTime) > DATE_ADD(PARSE_DATE('%Y%m%d',
        '${partitionDay}'), INTERVAL -30 DAY) ) asset
JOIN (
  SELECT
    g.customer.id customer_id,
    g.campaign.id campaign_id,
    g.customer.descriptivename customer_descriptive_name,
    g.campaign.name campaign_name,
    g.adGroup.id ad_group_id,
    g.adGroupAd.ad.id ad_group_ad_ad_id,
    DATE(g._partitionTime) date,
    CASE
      WHEN getApprovalStatus(g.adGroupAd.policySummary.approvalStatus) IS NULL THEN 'N/A'
    ELSE
    getApprovalStatus(g.adGroupAd.policySummary.approvalStatus)
  END
    adgroup_approval_status,
    getReviewStatus(g.adGroupAd.policySummary.reviewStatus) adgroup_review_status,
  FROM
    `${datasetId}.report_base_campaign_ads_approval` g
  WHERE
    getAdType(g.adGroupAd.ad.type) IN ('APP_AD',
      'APP_ENGAGEMENT_AD')
    AND DATE(g._partitionTime) > DATE_ADD(PARSE_DATE('%Y%m%d',
        '${partitionDay}'), INTERVAL -30 DAY) ) ag
ON
  asset.ad_group_id=ag.ad_group_id
  AND asset.date=ag.date