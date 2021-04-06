CREATE TEMP FUNCTION
  getPerformanceLabel(status INT64) AS (["UNSPECIFIED",
    "UNKNOWN",
    "PENDING",
    "LEARNING",
    "LOW",
    "GOOD",
    "BEST"][
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
SELECT
  DISTINCT customer_currency_code,
  campaign_name,
  customer_descriptive_name,
  campaign_status,
  customer_id,
  campaign_app_campaign_setting_app_id,
  campaign_app_campaign_setting_app_store,
  campaign_app_campaign_setting_bidding_strategy_goal_type,
  v.video_title,
  v.video_duration_millis,
  network.*,
  ad_groups,
  headline,
  image,
  description,
  video,
  adg_records,
  adg.metrics_impressions adg_impressions,
  adg.metrics_clicks adg_clicks,
  adg.metrics_cost adg_cost,
  adg.metrics_conversions adg_conversions,
  adg.metrics_conversions_value adg_conversions_value,
  adg.metrics_all_conversions adg_all_conversions,
  adg.metrics_all_conversions_value adg_all_conversions_value
FROM (
  SELECT
    campaign.id campaign_id,
    segments.date segments_date,
    adGroup.id adgroup_id,
    adGroup.name adgroup_name,
    getAdGroupStatus(adGroup.status) adgroup_status,
    getAdNetwork(segments.adNetworkType) segments_ad_network_type,
    asset.youtubeVideoAsset.youtubeVideoId asset_youtube_video_asset_youtube_video_id,
    getPerformanceLabel(adGroupAdAssetView.performanceLabel) ad_group_ad_asset_view_performance_label,
    asset.imageAsset.fullSize.widthPixels asset_image_asset_full_size_width_pixels,
    asset.imageAsset.fullSize.heightPixels asset_image_asset_full_size_height_pixels,
    asset.imageAsset.fullSize.url asset_image_asset_full_size_url,
    CASE
      WHEN getAssetFieldType(adGroupAdAssetView.fieldType) IN ("HEADLINE", "DESCRIPTION", "MANDATORY_AD_TEXT") THEN asset.textAsset.text
    --WHEN getAssetFieldType(adGroupAdAssetView.fieldType) = "YOUTUBE_VIDEO" THEN v.video_title
    ELSE
    asset.name
  END
    AS asset_name,
    asset.id asset_id,
    getAssetFieldType(adGroupAdAssetView.fieldType) ad_group_ad_asset_view_field_type,
    CASE
      WHEN getAssetFieldType(adGroupAdAssetView.fieldType) = "YOUTUBE_VIDEO" THEN CONCAT("https://www.youtube.com/watch?v=",asset.youtubeVideoAsset.youtubeVideoId)
      WHEN getAssetFieldType(adGroupAdAssetView.fieldType) = "MARKETING_IMAGE" THEN asset.imageAsset.fullSize.url
    ELSE
    NULL
  END
    AS asset_link,
    CASE
      WHEN getAssetFieldType(adGroupAdAssetView.fieldType) = "YOUTUBE_VIDEO" THEN CONCAT("https://i.ytimg.com/vi/",asset.youtubeVideoAsset.youtubeVideoId,"/hqdefault.jpg")
      WHEN getAssetFieldType(adGroupAdAssetView.fieldType) = "MARKETING_IMAGE" THEN asset.imageAsset.fullSize.url
    ELSE
    NULL
  END
    AS asset_thumbnail,
    SUM(metrics.clicks) metrics_clicks,
    SUM(metrics.conversionsValue) metrics_conversions_value,
    SUM(metrics.impressions) metrics_impressions,
    ROUND(SUM(metrics.costMicros)/1e6,2) metrics_cost,
    SUM(metrics.conversions) metrics_conversions,
    SUM(metrics.allConversions) metrics_all_conversions,
    SUM(metrics.allConversionsValue) metrics_all_conversions_value
  FROM
    `${datasetId}.report_app_asset_performance`
  WHERE
    DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
       '${partitionDay}')
    OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15,
    16 ) network
LEFT JOIN (
  select distinct video.id asset_youtube_video_asset_youtube_video_id, 
  video.title video_title,
  video.durationMillis video_duration_millis
  FROM `${datasetId}.report_app_videos`
) v using(asset_youtube_video_asset_youtube_video_id)
LEFT JOIN (
  SELECT
    adgroup_id,
    segments_date,
    SUM(adg_records) adg_records,
    SUM(headline)headline,
    SUM(description)description,
    SUM(image)image,
    SUM(video)video
  FROM (
    SELECT
      adGroup.id adgroup_id,
      segments.date segments_date,
      count(*) adg_records,
    IF
      ( getAssetFieldType(adGroupAdAssetView.fieldType) = "HEADLINE",
        COUNT(DISTINCT asset.id),
        0) headline,
    IF
      ( getAssetFieldType(adGroupAdAssetView.fieldType) = "DESCRIPTION",
        COUNT(DISTINCT asset.id),
        0) description,
    IF
      ( getAssetFieldType(adGroupAdAssetView.fieldType) = "MARKETING_IMAGE",
        COUNT(DISTINCT asset.id),
        0) image,
    IF
      ( getAssetFieldType(adGroupAdAssetView.fieldType) = "YOUTUBE_VIDEO",
        COUNT(DISTINCT asset.id),
        0) video
    FROM
      `${datasetId}.report_app_asset_performance`
    WHERE
      (DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
           '${partitionDay}')
        OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day))
      AND getAdGroupStatus(adGroup.status) = "ENABLED"
    GROUP BY
      adGroup.id,
      segments.date,
      adGroupAdAssetView.fieldType)
  GROUP BY
    1,
    2) g
USING
  (adgroup_id,
    segments_date)
LEFT JOIN (
  SELECT
    adgroup_id,
    segments_date,
    SUM( metrics_conversions ) metrics_conversions,
    SUM( metrics_conversions_value ) metrics_conversions_value,
    SUM( metrics_all_conversions) metrics_all_conversions,
    SUM( metrics_all_conversions_value ) metrics_all_conversions_value,
    ROUND(SUM( metrics_cost )/1e6,2) metrics_cost,
    SUM( metrics_impressions ) metrics_impressions,
    SUM( metrics_clicks ) metrics_clicks
  FROM (
    SELECT
      DISTINCT adGroup.id adgroup_id,
      segments.date segments_date,
      metrics.conversions metrics_conversions,
      metrics.conversionsValue metrics_conversions_value,
      metrics.allConversions metrics_all_conversions,
      metrics.allConversionsValue metrics_all_conversions_value,
      metrics.costMicros metrics_cost,
      metrics.impressions metrics_impressions,
      metrics.clicks metrics_clicks
    FROM
      `${datasetId}.report_app_ad_group_perf`
    WHERE
      (DATE(_partitionTime) = PARSE_DATE('%Y%m%d',
           '${partitionDay}')
        OR segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day))
      AND getAdGroupStatus(adGroup.status) = "ENABLED")
  GROUP BY
    adgroup_id,
    segments_date) adg
USING
  (adgroup_id,
    segments_date)
INNER JOIN
  `${datasetId}.app_snd_campaigns` camp
USING
  (campaign_id,
    segments_date)