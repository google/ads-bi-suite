CREATE TABLE impressions AS (
  SELECT user_id, campaign_id, count(*) as freq, sum(advertiser_impression_cost_usd) as imp_cost
  FROM adh.google_ads_impressions
  WHERE user_id IS NOT NULL
    AND CAST(campaign_id AS STRING) IN UNNEST(SPLIT("${campaignId}")) OR CAST(customer_id AS STRING) IN UNNEST(SPLIT("${customerId}"))
  GROUP BY 1 ,2
);

CREATE TABLE clicks AS (
  SELECT
    user_id,
    impression_data.campaign_id,
    SUM(advertiser_click_cost_usd) AS cost,
    COUNT(click_id.time_usec) AS clks
  FROM
    adh.google_ads_clicks
  WHERE user_id IS NOT NULL
    AND
    CAST(impression_data.campaign_id AS STRING) IN UNNEST(SPLIT("${campaignId}")) OR CAST(impression_data.customer_id AS STRING) IN UNNEST(SPLIT("${customerId}"))
    GROUP BY 1,2
  );

CREATE TABLE conversions AS (
  SELECT
    user_id,
    impression_data.campaign_id,
    count(conversion_id.time_usec) AS convs
  FROM
    adh.google_ads_conversions
  WHERE user_id IS NOT NULL
    AND CAST(conversion_type AS STRING) IN UNNEST(SPLIT("${conversionId}"))
    AND
    CAST(impression_data.campaign_id AS STRING) IN UNNEST(SPLIT("${campaignId}")) OR CAST(impression_data.customer_id AS STRING) IN UNNEST(SPLIT("${customerId}"))
  GROUP BY 1,2
  );

SELECT
  imp.campaign_id,
  t.campaign_name,
  imp.freq,
  sum(imp.freq) AS impression,
  count(distinct imp.user_id) AS unique_reach,
  sum(ck.cost) AS clk_cost,
  sum(imp.imp_cost) AS imp_cost,
  sum(IFNULL(ck.cost,0)+IFNULL(imp.imp_cost,0)) AS total_cost,
  sum(ck.clks) AS click,
  sum(conv.convs) AS conversion
FROM
  tmp.impressions imp
LEFT JOIN
  adh.google_ads_campaign t ON imp.campaign_id = t.campaign_id
LEFT JOIN
  tmp.clicks ck ON imp.user_id = ck.user_id AND imp.campaign_id = ck.campaign_id
LEFT JOIN
  tmp.conversions conv ON imp.user_id = conv.user_id AND imp.campaign_id = conv.campaign_id
GROUP BY
  1,2,3
ORDER BY
  freq ASC
