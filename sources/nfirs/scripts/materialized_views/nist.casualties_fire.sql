CREATE MATERIALIZED VIEW nist.casualties_fire AS
WITH c AS (
     SELECT ffcasualty.state, ffcasualty.fdid, ffcasualty.inc_date,
        ffcasualty.inc_no, ffcasualty.exp_no, 'ff' AS type,
        ffcasualty.ff_seq_no AS seq_no, ffcasualty.gender,
        ffcasualty.age, NULL AS race, NULL AS ethnicity,
            CASE
                WHEN ffcasualty.severity IN ('2', '3') THEN '1'
                WHEN ffcasualty.severity = '4' THEN '2'
                WHEN ffcasualty.severity = '5' THEN '3'
                WHEN ffcasualty.severity = '6' THEN '4'
                WHEN ffcasualty.severity = '7' THEN '5'
                ELSE ffcasualty.severity
            END AS sev
       FROM ffcasualty
       WHERE ffcasualty.severity <> '1'
  UNION
     SELECT civiliancasualty.state, civiliancasualty.fdid,
        civiliancasualty.inc_date, civiliancasualty.inc_no,
        civiliancasualty.exp_no, 'civ' AS type,
        civiliancasualty.seq_number AS seq_no,
        civiliancasualty.gender, civiliancasualty.age,
        civiliancasualty.race, civiliancasualty.ethnicity,
        civiliancasualty.sev
       FROM civiliancasualty
        )
  SELECT c.state, c.fdid, c.inc_date, c.inc_no, c.exp_no, c.type,
    c.seq_no, extract('year' from c.inc_date) as year,
    CASE
      WHEN b.aid IN('3', '4') THEN 'Y'::text
      ELSE 'N'::text
    END AS aid_flag,
    c.gender, c.age, c.race, c.ethnicity, c.sev,
    CASE
      WHEN b.prop_use LIKE '4%' THEN 'Y'
       ELSE 'N'
    END AS res,
    CASE
      WHEN ( extract('year' from b.inc_date) > 2001 AND b.inc_type IN ('111','120','121','122','123') OR extract('year' from b.inc_date) > 2001 AND extract('year' from b.inc_date) < 2008 AND b.inc_type = '112')
      AND f.struc_type IN ( '1', '2' )
        OR ( b.inc_type IN( '113', '114', '115', '116', '117', '118' ) OR b.inc_type = '110' AND extract('year' from b.inc_date) < 2009)
        AND ( f.struc_type IN( '1', '2' ) OR f.struc_type IS NULL ) THEN 'Y'::text
      ELSE 'N'::text
    END AS struc,
    CASE
      WHEN f.state IS NULL THEN 'N'
      ELSE 'Y'
    END AS module,
    CASE
      WHEN b.prop_use = '419' OR b.prop_use LIKE '9%' THEN 'Low Risk'
      WHEN b.prop_use NOT IN ( '419', '644', '645' ) AND substring(b.prop_use, 1, 1) IN ( '4', '5', '6', '7', '8' ) AND ( f.bldg_above IS NULL OR f.bldg_above::int < 7 ) THEN 'Med Risk'
      ELSE 'High Risk'
    END AS risk,
     CASE
       WHEN a.bkgpidfp10 IS NULL THEN NULL
       ELSE  '14000US'::text || "substring"(a.bkgpidfp10::text, 1, 11)
     END AS geoid,
     a.geom
    FROM c
    JOIN basicincident b USING (state, fdid, inc_date, inc_no, exp_no)
    LEFT JOIN incidentaddress a USING (state, fdid, inc_date, inc_no, exp_no)
    LEFT JOIN fireincident f USING (state, fdid, inc_date, inc_no, exp_no)
    WHERE inc_type like '1%' or f.state is not null;

GRANT ALL ON TABLE nist.casualties_fire TO sgilbert;
GRANT ALL ON TABLE nist.casualties_fire TO firecares;

