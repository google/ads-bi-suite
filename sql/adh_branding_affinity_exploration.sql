WITH imp_status as (
  SELECT
    user_id,
    customer_id,
    affinity_id
  FROM
    adh.google_ads_impressions,
    UNNEST (affinity) AS affinity_id
  WHERE user_id IS NOT NULL
  AND (CAST(campaign_id AS STRING) IN UNNEST(SPLIT("${campaignId}"))
  OR CAST(customer_id AS STRING) IN UNNEST(SPLIT("${customerId}"))) -- for cid or campaign ids
  GROUP BY 1,2,3
  ),

user_aff AS (
  SELECT
    user_id,
    customer_id,
    affinity_name, affinity_category
  FROM imp_status
  LEFT JOIN adh.affinity USING (affinity_id)
  GROUP BY 1,2,3,4
        ),

ac_impressions AS (
  SELECT
    user_id,
    customer_id,
    count(*) AS imps,
    SUM(advertiser_impression_cost_usd) as imp_cost
  FROM
    `adh.google_ads_impressions`
  WHERE
    user_id IS NOT NULL
    AND (CAST(campaign_id AS STRING) IN UNNEST(SPLIT("${campaignId}"))
    OR CAST(customer_id AS STRING) IN UNNEST(SPLIT("${customerId}")))
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
    AND (CAST(impression_data.campaign_id AS STRING) IN UNNEST(SPLIT("${campaignId}"))
    OR CAST(impression_data.customer_id AS STRING) IN UNNEST(SPLIT("${customerId}")))
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
          AND CAST(conversion_type AS STRING) IN UNNEST(SPLIT("${conversionId}"))
    AND (CAST(impression_data.campaign_id AS STRING) IN UNNEST(SPLIT("${campaignId}"))
    OR CAST(impression_data.customer_id AS STRING) IN UNNEST(SPLIT("${customerId}")))
  GROUP BY 1,2
  )

SELECT
  "${analysisName}" AS analysisName,
  uf.customer_id,
  customer_name,
  affinity_name, affinity_category,
  SUM(ai.imps) AS impression,
  COUNT(DISTINCT ai.user_id) AS unique_reach,
  SUM(ac.clks) AS clicks,
  SUM(ac.cost) AS cost,
  SUM(imp_cost) AS evc_cost,
  (IFNULL(SUM(ac.cost),0)+IFNULL(SUM(imp_cost),0)) AS total_cost,
  SUM(aconv.convs) AS conversions
FROM
    user_aff uf
LEFT JOIN
  ac_impressions ai ON uf.user_id = ai.user_id AND uf.customer_id = ai.customer_id
LEFT JOIN
  adh.google_ads_customer gac ON  uf.customer_id = gac.customer_id
LEFT JOIN
  ac_clicks ac ON uf.user_id = ac.user_id
                                                                              AND uf.customer_id = ac.customer_id
LEFT JOIN
  ac_conversions aconv ON uf.user_id = aconv.user_id
                                                                              AND uf.customer_id = aconv.customer_id
GROUP BY
  1,2,3,4,5
ORDER BY
 1,4 DESC