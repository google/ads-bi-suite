CREATE TABLE bi AS (
  SELECT
    UPPER(
      TO_HEX(
        MD5(
          UPPER(device_id)
        )
      )
    ) AS device_id,
    platform,
    CASE WHEN number_of_installs > 1 THEN 'reinstall user' WHEN number_of_installs = 1 THEN 'new user' END AS user_type,
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
    day30_revenue,
    number_of_installs
  FROM
    `${datasetId}.${firebaseTableName}`
)
