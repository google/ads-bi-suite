WITH imp_status as (
  SELECT
    user_id,
    customer_id,
    demographics.gender AS gender_id,
    demographics.age_group AS age_group_id
  FROM
    adh.google_ads_impressions
  WHERE user_id IS NOT NULL
  AND (campaign_id IN UNNEST(SPLIT("${campaign_id}"))  OR customer_id IN UNNEST(SPLIT("${customer_id}"))  -- for cid or campaign ids
  GROUP BY 1,2,3,4
  ),

user_demo AS (
  SELECT
    user_id,
    customer_id,
    gender_name,
    age_group_name
  FROM imp_status
  LEFT JOIN adh.gender USING (gender_id)
  LEFT JOIN adh.age_group USING (age_group_id)
  GROUP BY 1,2,3,4
        ),

ac_impressions AS (
  SELECT
    user_id,
    customer_id,
    COUNT(*) AS imps,
    SUM(advertiser_impression_cost_usd) as imp_cost
  FROM
    `adh.google_ads_impressions`
  WHERE
    user_id IS NOT NULL
    AND (campaign_id IN UNNEST(SPLIT("${campaign_id}")) OR customer_id IN UNNEST(SPLIT("${customer_id}"))
  GROUP BY 1,2
  ),

ac_clicks AS (
  SELECT
    user_id,
    impression_data.customer_id,
    COUNT(click_id.time_usec) AS clks,
    SUM(advertiser_click_cost_usd) AS cost
  FROM
    adh.google_ads_clicks
  WHERE
    user_id IS NOT NULL
    AND (impression_data.campaign_id IN UNNEST(SPLIT("${campaign_id}"))  OR impression_data.customer_id IN UNNEST(SPLIT("${customer_id}"))
  GROUP BY 1,2
  ),

ac_conversions AS (
  SELECT
    user_id,
    impression_data.customer_id,
    COUNT(conversion_id. time_usec) AS convs,
    MAX(conversion_id. time_usec) AS conv_time
  FROM
    adh.google_ads_conversions
  WHERE
    user_id IS NOT NULL
          AND conversion_type IN UNNEST(SPLIT("${conversion_id}"))
    AND (impression_data.campaign_id IN UNNEST(SPLIT("${campaign_id}"))  OR impression_data.customer_id IN UNNEST(SPLIT("${customer_id}"))
  GROUP BY 1,2
  )

SELECT
  ud.customer_id,
  customer_name,
  gender_name,
  age_group_name,
  SUM(ai.imps) AS impression,
  COUNT(DISTINCT ai.user_id) AS unique_reach,
  SUM(ac.clks) AS clicks,
  SUM(ac.cost) AS cost,
  SUM(imp_cost) AS evc_cost,
  (IFNULL(SUM(ac.cost),0)+IFNULL(SUM(imp_cost),0)) AS total_cost,
  SUM(aconv.convs) AS conversions
FROM
    user_demo ud
LEFT JOIN
  ac_impressions ai ON ud.user_id = ai.user_id AND ud.customer_id = ai.customer_id
LEFT JOIN
  adh.google_ads_customer gac ON  ud.customer_id = gac.customer_id
LEFT JOIN
  ac_clicks ac ON ud.user_id = ac.user_id
                                                                              AND ud.customer_id = ac.customer_id
LEFT JOIN
  ac_conversions aconv ON ud.user_id = aconv.user_id
                                                                              AND ud.customer_id = aconv.customer_id
GROUP BY
  1,2,3,4
ORDER BY
 1,4 DESC