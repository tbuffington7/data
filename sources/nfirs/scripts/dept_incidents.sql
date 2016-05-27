/*

# Create the dept_incidents intermediate table

* Its purpose is to provide the data needed to correct for geolocation errors.
* Note this view needs to be refreshed whenever new NFIRS data is added or
  when records are geolocated. (REFRESH MATERIALIZED VIEW nist.dept_incidents;)


*/


CREATE MATERIALIZED VIEW nist.dept_incidents AS
WITH t AS (
SELECT b.state, b.fdid, extract('year' from b.inc_date) AS year,
  CASE
    WHEN a.geom IS NOT NULL THEN 1
    ELSE 0
  END AS located,
  CASE
    WHEN b.inc_type LIKE '1%' OR f.state IS NOT NULL THEN 1
    ELSE 0
  END AS fire,
  CASE
    WHEN b.version::numeric(4,1) = 5.0::numeric(4,1) THEN 1
    ELSE 0
  END AS version5,
  CASE
    WHEN b.aid IN ( '3', '4' ) THEN 0
    ELSE 1
  END AS aid,
  CASE
    WHEN ( extract('year' from b.inc_date) > 2001 AND b.inc_type IN ( '111', '120', '121', '122', '123' )
    OR extract('year' from b.inc_date) > 2001 AND extract('year' from b.inc_date) < 2008 AND b.inc_type = '112') AND f.struc_type IN ( '1', '2' )
    OR ( b.inc_type IN ( '113', '114', '115', '116', '117', '118' ) OR b.inc_type = '110' AND extract('year' from b.inc_date) < 2009) AND ( f.struc_type IN ( '1', '2' ) OR f.struc_type IS NULL ) THEN 1
  ELSE 0
  END AS struc,
  CASE
    WHEN f.state IS NOT NULL THEN 1
    ELSE 0
    END AS module,
  CASE
    WHEN ( f.not_res = 'N' OR b.prop_use LIKE '4%' ) THEN 1
    ELSE 0
  END AS res,
  CASE
    WHEN b.prop_use = '419' OR b.prop_use LIKE '9%' THEN 1
    ELSE 0
  END AS lr,
  b.ff_inj, b.oth_inj, b.ff_death, b.oth_death
FROM basicincident b
LEFT JOIN incidentaddress a USING ( state, fdid, inc_date, inc_no, exp_no)
LEFT JOIN fireincident f USING ( state, fdid, inc_date, inc_no, exp_no)
)

SELECT
  t.state,
  t.fdid,
  t.year,
  sum(t.aid) AS incidents,
  sum(t.aid * t.located) AS incidents_loc,
  sum(t.aid * t.version5) AS v5_incidents,
  sum(t.aid * t.version5 * t.located) AS v5_incidents_loc,
  sum(t.aid * t.fire) AS fires,
  sum(t.aid * t.located * t.fire) AS fires_loc,
  sum(t.aid * t.module) AS mod_fires,
  sum(t.aid * t.located * t.module) AS mod_fires_loc,
  sum(t.aid * t.struc) AS struc_fires,
  sum(t.aid * t.located * t.struc) AS struc_fires_loc,
  sum(t.aid * t.res * t.struc) AS res_fires,
  sum(t.aid * t.located * t.res * t.struc) AS res_fires_loc,
  sum(t.aid * t.lr * t.struc) AS lr_fires,
  sum(t.aid * t.located * t.lr * t.struc) AS lr_fires_loc,
  sum(t.ff_inj + t.oth_inj * t.aid) AS injuries,
  sum(t.located * (t.ff_inj + t.oth_inj * t.aid)) AS injuries_loc,
  sum(t.ff_death + t.oth_death * t.aid) AS deaths,
  sum(t.located * (t.ff_death + t.oth_death * t.aid)) AS deaths_loc
FROM t
GROUP BY t.state, t.fdid, t.year;


CREATE UNIQUE INDEX dept_incidents_pk
  ON nist.dept_incidents (state, fdid, year);
