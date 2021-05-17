CREATE TABLE affinity AS (
  SELECT
    UPPER(device_id_md5) AS device_id,
    affinity_name,
  FROM
    adh.google_ads_impressions_rdid,
    UNNEST (affinity) AS affinity_id
    LEFT JOIN adh.affinity USING (affinity_id)
  WHERE
    device_id_md5 IS NOT NULL
);
SELECT
  user_type,
  platform,
  affinity_name,
  COUNT(distinct device_id) AS users,
  SUM(
    IFNULL(day2_retention, 0)
  ) AS day2_retention,
  SUM(
    IFNULL(day3_retention, 0)
  ) AS day3_retention,
  SUM(
    IFNULL(day4_retention, 0)
  ) AS day4_retention,
  SUM(
    IFNULL(day5_retention, 0)
  ) AS day5_retention,
  SUM(
    IFNULL(day6_retention, 0)
  ) AS day6_retention,
  SUM(
    IFNULL(day7_retention, 0)
  ) AS day7_retention,
  SUM(
    IFNULL(day1_revenue, 0)
  ) AS day1_revenue,
  SUM(
    IFNULL(day2_revenue, 0)
  ) AS day2_revenue,
  SUM(
    IFNULL(day3_revenue, 0)
  ) AS day3_revenue,
  SUM(
    IFNULL(day4_revenue, 0)
  ) AS day4_revenue,
  SUM(
    IFNULL(day5_revenue, 0)
  ) AS day5_revenue,
  SUM(
    IFNULL(day6_revenue, 0)
  ) AS day6_revenue,
  SUM(
    IFNULL(day7_revenue, 0)
  ) AS day7_revenue,
  SUM(
    IFNULL(day14_revenue, 0)
  ) AS day14_revenue,
  SUM(
    IFNULL(day30_revenue, 0)
  ) AS day30_revenue
FROM
  tmp.bi
  INNER JOIN tmp.affinity USING (device_id)
GROUP BY
  1,
  2,
  3
