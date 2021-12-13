WITH mh_disc_users AS (
  SELECT
    device_id_md5,
    1 AS user_type
  FROM adh.yt_reserve_impressions_rdid
  WHERE media_plan_id IN (
# branding media plan ids here
11
)
  AND device_id_md5 IS NOT NULL
  GROUP BY 1
  ),

ac_impressions AS (
  SELECT
    device_id_md5,
    customer_id,
    COUNT(*) AS imps,
    SUM(advertiser_impression_cost_usd) as imp_cost
  FROM
    `adh.google_ads_impressions_rdid`
  WHERE
    device_id_md5 IS NOT NULL
    AND (campaign_id IN UNNEST(SPLIT("${campaign_id}")) OR customer_id IN UNNEST(SPLIT("${customer_id}"))
  GROUP BY 1,2
  ),

ac_clicks AS (
  SELECT
    device_id_md5,
    impression_data.customer_id,
    COUNT(click_id.time_usec) AS clks,
    SUM(advertiser_click_cost_usd) AS cost
  FROM
    adh.google_ads_clicks_rdid
  WHERE
    device_id_md5 IS NOT NULL
    AND (impression_data.campaign_id IN UNNEST(SPLIT("${campaign_id}")) OR customer_id IN UNNEST(SPLIT("${customer_id}"))
  GROUP BY 1,2
  ),

ac_conversions AS (
  SELECT
    device_id_md5,
    impression_data.customer_id,
    COUNT(conversion_id. time_usec) AS convs
  FROM
    adh.google_ads_conversions_rdid
  WHERE
    device_id_md5 IS NOT NULL
          AND conversion_type IN UNNEST(SPLIT("${conversion_id}"))
    AND (impression_data.campaign_id IN UNNEST(SPLIT("${campaign_id}")) OR customer_id IN UNNEST(SPLIT("${customer_id}"))
  GROUP BY 1,2
  )

SELECT
  ai.customer_id,
  customer_name,
  CASE
    WHEN yt.user_type = 1
    THEN "wt_yt"
    ELSE "ac_only"
  END AS user_type,
  SUM(ai.imps) AS impression,
  COUNT(DISTINCT ai.device_id_md5) AS unique_reach,
  SUM(ac.clks) AS clicks,
  SUM(ac.cost) AS cost,
  SUM(imp_cost) AS evc_cost,
  (IFNULL(SUM(ac.cost),0)+IFNULL(SUM(imp_cost),0)) AS total_cost,
  SUM(aconv.convs) AS conversions
FROM
  ac_impressions ai
LEFT JOIN
  mh_disc_users yt ON ai.device_id_md5 = yt.device_id_md5
LEFT JOIN
  adh.google_ads_customer USING (customer_id)
LEFT JOIN
  ac_clicks ac ON ai.device_id_md5 = ac.device_id_md5
                                                                              AND ai.customer_id = ac.customer_id
LEFT JOIN
  ac_conversions aconv ON ai.device_id_md5 = aconv.device_id_md5
                                                                              AND ai.customer_id = aconv.customer_id
GROUP BY
  1,2,3
ORDER BY
 1,4 DESC