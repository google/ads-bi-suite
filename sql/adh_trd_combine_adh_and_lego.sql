SELECT
  l.*,
  c.cpi,
  users,
  day2_retention,
  day3_retention,
  day4_retention,
  day5_retention,
  day6_retention,
  day7_retention,
  day1_revenue,
  day2_revenue,
  day3_revenue,
  day4_revenue,
  day5_revenue,
  day6_revenue,
  day7_revenue,
  day14_revenue,
  day30_revenue
FROM
  (
    SELECT
      campaign_id,
      campaign_name,
      asset_youtube_video_asset_youtube_video_id video_id,
      segments_ad_network_type,
      SUM(metrics_clicks) clicks,
      SUM(metrics_impressions) impressions,
      SUM(metrics_conversions) conversions,
      SUM(metrics_conversions_value) conversion_value,
      SUM(metrics_cost) metrics_cost
    FROM
      `${adsDatasetId}.app_trd_asset_perf_report`
    WHERE
      asset_youtube_video_asset_youtube_video_id IS NOT NULL
    GROUP BY
      1,
      2,
      3,
      4
  ) l
  LEFT JOIN (
    SELECT
      campaign_id,
      segments_ad_network_type,
      IF(
        SUM(installs) > 0,
        SUM(metrics_cost)/ SUM(installs),
        0
      ) cpi
    FROM
      `${adsDatasetId}.app_trd_campaign_perf_report`
    GROUP BY
      1,
      2
  ) c USING (
    campaign_id, segments_ad_network_type
  )
  LEFT JOIN (
    SELECT
      *,
      IF (
        inventory = "YOUTUBE", "YOUTUBE_WATCH",
        "CONTENT"
      ) segments_ad_network_type
    FROM
      `${datasetId}.adh_snd_video_asset_with_lego_*`
      WHERE  _TABLE_SUFFIX='${partitionDay}'
  ) a USING (
    campaign_id, video_id, segments_ad_network_type
  )