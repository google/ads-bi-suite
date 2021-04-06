select
  customer.id as Customer_id,
  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) as string), metrics.costMicros, 0)/ 1000000) as week1_cost,
  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) as string), metrics.costMicros, 0)/ 1000000) as week2_cost,
  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) as string), metrics.costMicros, 0)) / nullif(sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) as string), metrics.costMicros, 0)),0) - 1 as cost_wow,
  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) as string), metrics.clicks, 0)) / nullif(sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) as string), metrics.clicks, 0)),0) - 1 as clicks_wow,
  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) as string), metrics.conversions, 0)) / nullif(sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) as string), metrics.conversions, 0)),0) - 1 as conversions_wow,

  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) as string), metrics.clicks, 0)) as week1_clicks,
  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) as string), metrics.clicks, 0)) as week2_clicks,

  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) as string), metrics.conversions, 0)) as week1_conversions,
  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) as string), metrics.conversions, 0)) as week2_conversions,

  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) as string), metrics.conversionsValue, 0)) as week1_conversion_value,
  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) as string), metrics.conversionsValue, 0)) as week2_conversion_value,

  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 7 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 1 DAY) as string), metrics.allConversionsValue, 0)) as week1_all_conversion_value,
  sum(if (cast(segments.date as string) between cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 14 DAY) as string) AND cast(DATE_SUB(PARSE_DATE('%Y%m%d', '${partitionDay}'), INTERVAL 8 DAY) as string), metrics.allConversionsValue, 0)) as week2_all_conversion_value

from  `${datasetId}.report_base_account_performance`

where DATE(_partitionTime) = PARSE_DATE('%Y%m%d', '${partitionDay}')
group by
  Customer_id
