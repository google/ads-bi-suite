WITH
  cc AS(
  SELECT
    segments_date,
    c.*
  FROM (
    SELECT
      DISTINCT segments.date segments_date
    FROM
      `${datasetId}.report_app_ad_group` adg
    INNER JOIN (
      SELECT
        DATE_ADD(DATE(MIN(_PARTITIONTIME)), INTERVAL -1 day) launch_date
      FROM
        `${datasetId}.report_app_campaign_criterion`)
    ON
      segments.date < launch_date
    WHERE
      DATE(adg._partitionTime) = PARSE_DATE('%Y%m%d',
        '${partitionDay}')
      OR adg.segments.date = DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -31 day))
  LEFT JOIN (
    SELECT
      DISTINCT campaign.id campaign_id,
      campaignCriterion.LANGUAGE.languageConstant languageConstant,
      campaignCriterion.location.geoTargetConstant geoTargetConstant
    FROM
      `${datasetId}.report_app_campaign_criterion` c
    WHERE
      campaignCriterion.negative = FALSE
      AND _PARTITIONTIME IN (
      SELECT
        MIN(_PARTITIONTIME)
      FROM
        `${datasetId}.report_app_campaign_criterion`) ) c
  ON
    1=1
  UNION ALL (
    SELECT
      DISTINCT DATE_ADD(DATE(_PARTITIONTIME), INTERVAL -1 day) AS segments_date,
      campaign.id campaign_id,
      campaignCriterion.LANGUAGE.languageConstant languageConstant,
      campaignCriterion.location.geoTargetConstant geoTargetConstant
    FROM
      `${datasetId}.report_app_campaign_criterion`
    WHERE
      campaignCriterion.negative = FALSE) )
SELECT
  camp.*,
  l.languageConstant.name language_name,
  l.languageConstant.code language_code,
  g.geoTargetConstant.canonicalName geoTargetConstant_canonicalName,
  g.geoTargetConstant.countryCode geoTargetConstant_countryCode
FROM
  `${datasetId}.app_snd_campaigns` camp
LEFT JOIN (
  SELECT
    campaign_id,
    languageConstant,
    geoTargetConstant,
    segments_date
  FROM
    cc ) c
USING
  (campaign_id,
    segments_date)
LEFT JOIN
  `${datasetId}.report_base_language_constant` l
ON
  l.languageConstant.resourceName = c.languageConstant
LEFT JOIN
  `${datasetId}.report_base_geo_target_constant` g
ON
  g.geoTargetConstant.resourceName =c.geoTargetConstant
WHERE
  campaign_status = "ENABLED"