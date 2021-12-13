# for trueview and AC campaigns
WITH yt_disc_users AS (
  SELECT
    user_id,
    1 AS user_type
  FROM adh.google_ads_impressions
  WHERE (campaign_id IN UNNEST(SPLIT("${yt_campaign_id}")) OR customer_id IN UNNEST(SPLIT("${yt_customer_id}")) -- for youtube cid or campaign ids
  AND user_id IS NOT NULL
  GROUP BY 1
  ),

ac_impressions AS (
  SELECT
    user_id,
    customer_id,
    count(*) AS imps,
    sum(advertiser_impression_cost_usd) as imp_cost
  FROM
    `adh.google_ads_impressions`
  WHERE
    user_id IS NOT NULL
    AND (campaign_id IN UNNEST(SPLIT("${campaign_id}")) OR customer_id IN UNNEST(SPLIT("${customer_id}"))-- for AC cid or campaign ids
  GROUP BY 1,2
  ),

ac_clicks AS (
  SELECT
    user_id,
    impression_data.customer_id,
    count(click_id.time_usec) AS clks,
    sum(advertiser_click_cost_usd) AS cost
  FROM
    adh.google_ads_clicks
  WHERE
    user_id IS NOT NULL
    AND (impression_data.campaign_id IN UNNEST(SPLIT("${campaign_id}")) OR impression_data.customer_id IN UNNEST(SPLIT("${customer_id}")) -- for AC cid or campaign ids
  GROUP BY 1,2
  ),

ac_conversions AS (
  SELECT
    user_id,
    impression_data.customer_id,
    count(conversion_id. time_usec) AS convs
  FROM
    adh.google_ads_conversions
  WHERE
    user_id IS NOT NULL
          AND conversion_type IN UNNEST(SPLIT("${conversion_id}")) -- for AC conversion ids, can be found via 1) google ads api 2) google teams
    AND (impression_data.campaign_id IN UNNEST(SPLIT("${campaign_id}")) OR impression_data.customer_id IN UNNEST(SPLIT("${customer_id}")) -- for AC cid or campaign ids
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
  sum(ai.imps) AS impression,
  count(DISTINCT ai.user_id) AS unique_reach,
  sum(ac.clks) AS clicks,
  sum(ac.cost) AS cost,
  sum(imp_cost) AS evc_cost,
  (ifnull(sum(ac.cost),0)+ifnull(sum(imp_cost),0)) AS total_cost,
  sum(aconv.convs) AS conversions
FROM
  ac_impressions ai
LEFT JOIN
  yt_disc_users yt ON ai.user_id = yt.user_id
LEFT JOIN
  adh.google_ads_customer USING (customer_id)
LEFT JOIN
  ac_clicks ac ON ai.user_id = ac.user_id
                                                                              AND ai.customer_id = ac.customer_id
LEFT JOIN
  ac_conversions aconv ON ai.user_id = aconv.user_id
                                                                              AND ai.customer_id = aconv.customer_id
GROUP BY
  1,2,3
ORDER BY
 1,4 DESC