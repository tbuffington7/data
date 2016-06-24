CREATE MATERIALIZED VIEW nist.dept_incidents AS
WITH t AS (
         SELECT b.state, b.fdid, EXTRACT('year' from b.inc_date) AS year,
                CASE
                    WHEN a.geom IS NOT NULL THEN 1
                    ELSE 0
                END AS located,
                CASE
                    WHEN b.inc_type like '1%' OR f.state IS NOT NULL THEN 1
                    ELSE 0
                END AS fire,
                CASE
                    WHEN f.state IS NOT NULL THEN 1
                    ELSE 0
                END AS module,
                CASE
                    WHEN f.state IS NOT NULL AND (f.not_res = 'N' OR b.prop_use like '4%' ) THEN 1
                    ELSE 0
                END AS res,
                b.ff_inj + b.oth_inj AS injury, b.ff_death + b.oth_death AS death
           FROM basicincident b LEFT JOIN incidentaddress a USING (state, fdid, inc_date, inc_no, exp_no)
   LEFT JOIN fireincident f USING (state, fdid, inc_date, inc_no, exp_no)
        )
 SELECT t.state, t.fdid, t.year,
    count(*) AS incidents, sum(t.located) AS incidents_loc,
    sum(t.fire) AS fires, sum(t.located * t.fire) AS fires_loc,
    sum(t.module) AS mod_fires, sum(t.located * t.module) AS mod_fires_loc,
    sum(t.res) AS res_fires, sum(t.located * t.res) AS res_fires_loc,
    sum(t.injury) AS injuries, sum(t.located * t.injury) AS injuries_loc,
    sum(t.death) AS deaths, sum(t.located * t.death) AS deaths_loc
   FROM t
  GROUP BY t.state, t.fdid, t.year;
