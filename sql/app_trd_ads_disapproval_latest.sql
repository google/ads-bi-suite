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
CREATE TEMP FUNCTION
  getTopicEntryType(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "PROHIBITED",
    "",
    "LIMITED",
    "DESCRIPTIVE",
    "BROADENING",
    "AREA_OF_INTEREST_ONLY",
    "FULLY_LIMITED"][
  OFFSET
    (status)]);
CREATE TEMP FUNCTION
  getAssetType(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "YOUTUBE_VIDEO",
    "MEDIA_BUNDLE",
    "IMAGE",
    "TEXT",
    "BOOK_ON_GOOGLE"][
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
SELECT
  cur.*,
  pre.adgroup_approval_status l_adgroup_approval_status,
  pre.asset_approval_status l_asset_approval_status,
  pre.adgroup_review_status l_adgroup_review_status,
  pre.asset_review_status l_asset_review_status,
  pre.partition_time l__partitionTime,
FROM (
  SELECT
    customer_id,
    campaign_id,
    customer_descriptive_name,
    campaign_name,
    ad_group_id,
    ad.ad_group_ad_ad_id,
    type,
    topic,
    adgroup_approval_status,
    adgroup_review_status,
    --ad_group_ad_ad_type,
    ad_group_name,
    partition_time,
    asset_approval_status,
    asset_review_status,
    ad_group_ad_asset_view_field_type,
    asset_id
  FROM (
    SELECT
      g.customer.id customer_id,
      g.customer.descriptivename customer_descriptive_name,
      g.campaign.id campaign_id,
      g.campaign.name campaign_name,
      g.adGroup.id ad_group_id,
      g.adGroupAd.ad.id ad_group_ad_ad_id,
      getTopicEntryType(v.type) type,
      v.topic,
      CASE
        WHEN getApprovalStatus(g.adGroupAd.policySummary.approvalStatus) IS NULL THEN 'N/A'
      ELSE
      getApprovalStatus(g.adGroupAd.policySummary.approvalStatus)
    END
      adgroup_approval_status,
      getReviewStatus(g.adGroupAd.policySummary.reviewStatus) adgroup_review_status,
      g.adGroupAd.ad.type ad_group_ad_ad_type,
      g.adGroup.name ad_group_name,
      DATE(g._partitionTime) AS partition_time
    FROM
      `${datasetId}.report_base_campaign_ads_approval` g
    LEFT JOIN
      UNNEST(g.adGroupAd.policySummary.policyTopicEntries) AS v
    WHERE
      DATE(g._partitionTime) = PARSE_DATE('%Y%m%d',
        '${partitionDay}')) ad --PARSE_DATE('%Y%m%d','${partitionDay}')
  JOIN (
    SELECT
      CASE
        WHEN getApprovalStatus(s.adGroupAdAssetView.policySummary.approvalStatus ) IS NULL THEN 'N/A'
      ELSE
      getApprovalStatus(s.adGroupAdAssetView.policySummary.approvalStatus)
    END
      asset_approval_status,
      getReviewStatus(s.adGroupAdAssetView.policySummary.reviewStatus ) asset_review_status,
      getAssetFieldType(s.adGroupAdAssetView.fieldType) ad_group_ad_asset_view_field_type,
      s.asset.id asset_id,
      s.adGroupAd.ad.id ad_group_ad_ad_id,
    FROM
      `${datasetId}.report_app_disapprovals_ad_group_ad_asset_view` s
    WHERE
      DATE(s._partitionTime) = PARSE_DATE('%Y%m%d',
        '${partitionDay}')) asset
  ON
    asset.ad_group_ad_ad_id=ad.ad_group_ad_ad_id) cur
LEFT JOIN (
  SELECT
    customer_id,
    campaign_id,
    ad_group_id,
    ad.ad_group_ad_ad_id,
    type,
    topic,
    adgroup_approval_status,
    adgroup_review_status,
    --ad_group_ad_ad_type,
    ad_group_name,
    partition_time,
    asset_approval_status,
    asset_review_status,
    ad_group_ad_asset_view_field_type,
    asset_id,
    asset_type
  FROM (
    SELECT
      g.customer.id customer_id,
      g.campaign.id campaign_id,
      g.adGroup.id ad_group_id,
      g.adGroupAd.ad.id ad_group_ad_ad_id,
      getTopicEntryType(v.type) type,
      v.topic,
      CASE
        WHEN getApprovalStatus(g.adGroupAd.policySummary.approvalStatus) IS NULL THEN 'N/A'
      ELSE
      getApprovalStatus(g.adGroupAd.policySummary.approvalStatus)
    END
      adgroup_approval_status,
      getReviewStatus(g.adGroupAd.policySummary.reviewStatus ) adgroup_review_status,
      g.adGroupAd.ad.type ad_group_ad_ad_type,
      g.adGroup.name ad_group_name,
      DATE(g._partitionTime) partition_time,
    FROM
      `${datasetId}.report_base_campaign_ads_approval` g
    LEFT JOIN
      UNNEST(g.adGroupAd.policySummary.policyTopicEntries ) AS v
    WHERE
      DATE(g._partitionTime) = DATE_ADD(PARSE_DATE('%Y%m%d',
          '${partitionDay}'), INTERVAL -1 DAY)
      AND getAdType(g.adGroupAd.ad.type) IN ('APP_AD',
        'APP_ENGAGEMENT_AD') ) ad
  JOIN (
    SELECT
      CASE
        WHEN getApprovalStatus(s.adGroupAdAssetView.policySummary.approvalStatus ) IS NULL THEN 'N/A'
      ELSE
      getApprovalStatus(s.adGroupAdAssetView.policySummary.approvalStatus)
    END
      asset_approval_status,
      getReviewStatus(s.adGroupAdAssetView.policySummary.reviewStatus) asset_review_status,
      s.adGroupAdAssetView.fieldType ad_group_ad_asset_view_field_type,
      s.asset.id asset_id,
      getAssetType(s.asset.type) asset_type,
      s.adGroupAd.ad.id ad_group_ad_ad_id,
    FROM
      `${datasetId}.report_app_disapprovals_ad_group_ad_asset_view` s
    WHERE
      DATE(_partitionTime) = DATE_ADD(PARSE_DATE('%Y%m%d',
          '${partitionDay}'), INTERVAL -1 DAY)) asset
  ON
    asset.ad_group_ad_ad_id=ad.ad_group_ad_ad_id ) pre
ON
  (pre.ad_group_id = cur.ad_group_id
    AND pre.ad_group_ad_ad_id = cur.ad_group_ad_ad_id
    AND pre.asset_id = cur.asset_id) ;