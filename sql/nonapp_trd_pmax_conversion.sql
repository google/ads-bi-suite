WITH
  base AS (
    SELECT *
    FROM `${datasetId}.report_nonapp_pmax_conversion`
    WHERE
      _PARTITIONTIME IN (
        SELECT MAX(_partitionTime) FROM `${datasetId}.report_nonapp_pmax_conversion`
      )
  ),
  conv_value AS (
    SELECT DISTINCT
      customer.id AS Customer_ID,
      sum(metrics.all_conversions_value) AS conv_value
    FROM base
    GROUP BY 1
  ),
  dda AS (
    SELECT DISTINCT
      customer.id AS Customer_ID,
      STRING_AGG(conversion_action.attribution_model_settings.attribution_model) AS dda
    FROM base
    GROUP BY 1
  ),
  oci AS (
    SELECT DISTINCT
      customer.id AS Customer_ID,
      STRING_AGG(conversion_action.type) AS oci
    FROM base
    GROUP BY 1
  ),
  pmax_campaign_daily_spend AS (
    SELECT
      customer_id,
      avg(cost) AS pmax_spend_7d
    FROM
      (
        SELECT DISTINCT
          customer_id,
          segments_date,
          sum(metrics_cost) AS cost
        FROM `${datasetId}.base_snd_campaign_performance`
        WHERE
          segments_date < PARSE_DATE('%Y%m%d', '${partitionDay}')
          AND segments_date > date_sub(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 day)
          AND advertising_channel_type = 'PERFORMANCE_MAX'
        GROUP BY 1, 2
      )
    GROUP BY 1
  )
SELECT DISTINCT
  customer.id AS Customer_ID,
  customer.currency_code AS Currency,
  customer.descriptive_name AS Account,
  CASE WHEN cv.conv_value > 0 THEN TRUE ELSE FALSE END AS has_conv_value,
  CASE WHEN REGEXP_CONTAINS(d.dda, 'DATA_DRIVEN') THEN TRUE ELSE FALSE END AS has_dda,
  CASE WHEN REGEXP_CONTAINS(o.oci, 'UPLOAD') THEN TRUE ELSE FALSE END AS has_oci,
  p.pmax_spend_7d
FROM base b
LEFT JOIN conv_value cv
  ON b.customer.id = cv.Customer_ID
LEFT JOIN dda d
  ON b.customer.id = d.Customer_ID
LEFT JOIN oci o
  ON b.customer.id = o.Customer_ID
LEFT JOIN pmax_campaign_daily_spend p
  ON b.customer.id = p.customer_id