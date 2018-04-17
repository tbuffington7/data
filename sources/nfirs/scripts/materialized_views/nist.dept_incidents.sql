/*

# Create the dept_incidents intermediate table

* Its purpose is to provide the data needed to correct for geolocation errors.
* Note this view needs to be refreshed whenever new NFIRS data is added or
  when records are geolocated. (REFRESH MATERIALIZED VIEW nist.dept_incidents;)


*/
CREATE MATERIALIZED VIEW public.dept_incidents2 AS 
 WITH t0 AS (
         SELECT b.state,
            b.fdid,
            b.inc_date % 10000 AS year,
                CASE
                    WHEN a.geom IS NOT NULL THEN 1
                    ELSE 0
                END AS located,
                CASE
                    WHEN b.inc_type ~~ '1%'::text OR f.state IS NOT NULL THEN 1
                    ELSE 0
                END AS fire,
                CASE
                    WHEN b.version::numeric(4,1) = 5.0::numeric(4,1) THEN 1
                    ELSE 0
                END AS version5,
                CASE
                    WHEN b.aid = ANY (ARRAY['3'::text, '4'::text]) THEN 0
                    ELSE 1
                END AS aid,
                CASE
                    WHEN ((b.inc_date % 10000) > 2001 AND (b.inc_type = ANY (ARRAY['111'::text, '120'::text, '121'::text, '122'::text, '123'::text])) OR (b.inc_date % 10000) > 2001 AND (b.inc_date % 10000) < 2008 AND b.inc_type = '112'::text) AND (f.struc_type = ANY (ARRAY['1'::text, '2'::text])) OR ((b.inc_type = ANY (ARRAY['113'::text, '114'::text, '115'::text, '116'::text, '117'::text, '118'::text])) OR b.inc_type = '110'::text AND (b.inc_date % 10000) < 2009) AND ((f.struc_type = ANY (ARRAY['1'::text, '2'::text])) OR f.struc_type IS NULL) THEN 1
                    ELSE 0
                END AS struc,
                CASE
                    WHEN f.state IS NOT NULL THEN 1
                    ELSE 0
                END AS module,
                CASE
                    WHEN f.not_res = 'N'::text OR b.prop_use ~~ '4%'::text THEN 1
                    ELSE 0
                END AS res,
                CASE
                    WHEN b.prop_use = '419'::text OR b.prop_use ~~ '9%'::text THEN 1
                    ELSE 0
                END AS lr,
            b.ff_inj,
            b.oth_inj,
            b.ff_death,
            b.oth_death
           FROM basicincident b
             LEFT JOIN incidentaddress a USING ( state, fdid, inc_date, inc_no, exp_no)
             LEFT JOIN fireincident f USING ( state, fdid, inc_date, inc_no, exp_no)
        ), t AS (
         SELECT t0.state,
            t0.fdid,
            t0.year,
            sum(t0.aid) AS incidents,
            sum(t0.aid * t0.located) AS incidents_loc,
            sum(t0.aid * t0.version5) AS v5_incidents,
            sum(t0.aid * t0.version5 * t0.located) AS v5_incidents_loc,
            sum(t0.aid * t0.fire) AS fires,
            sum(t0.aid * t0.located * t0.fire) AS fires_loc,
            sum(t0.aid * t0.module) AS mod_fires,
            sum(t0.aid * t0.located * t0.module) AS mod_fires_loc,
            sum(t0.aid * t0.struc) AS struc_fires,
            sum(t0.aid * t0.located * t0.struc) AS struc_fires_loc,
            sum(t0.aid * t0.res * t0.struc) AS res_fires,
            sum(t0.aid * t0.located * t0.res * t0.struc) AS res_fires_loc,
            sum(t0.aid * t0.lr * t0.struc) AS lr_fires,
            sum(t0.aid * t0.located * t0.lr * t0.struc) AS lr_fires_loc,
            sum(t0.ff_inj + t0.oth_inj * t0.aid) AS injuries,
            sum(t0.located * (t0.ff_inj + t0.oth_inj * t0.aid)) AS injuries_loc,
            sum(t0.ff_death + t0.oth_death * t0.aid) AS deaths,
            sum(t0.located * (t0.ff_death + t0.oth_death * t0.aid)) AS deaths_loc
           FROM t0
          GROUP BY t0.state, t0.fdid, t0.year
        ), e_l AS (
		 SELECT 
           state, 
		   fdid, 
		   inc_date,
		   inc_no, 
		   exp_no, 
		   addr_type
         FROM ems12_geocode
		 UNION
		 SELECT 
           state, 
		   fdid, 
		   inc_date,
		   inc_no, 
		   exp_no, 
		   addr_type
		 FROM ems13_geocode
		 UNION
		 SELECT 
           state, 
		   fdid, 
		   inc_date,
		   inc_no, 
		   exp_no, 
		   addr_type
		 FROM ems14_geocode
		 UNION
		 SELECT 
           state, 
		   fdid, 
		   inc_date,
		   inc_no, 
		   exp_no, 
		   addr_type
		 FROM ems15_geocode
		), e AS (
         SELECT ems.state,
            ems.fdid,
            "substring"(ems.inc_date, 5, 4)::integer AS year,
            count(*) AS calls,
            sum(
                CASE
                    WHEN e_l.addr_type IN ('PointAddress', 'StreetAddress', 'StreetInd') THEN 1
                    ELSE 0
                END) AS calls_loc_s,
            sum(
                CASE
                    WHEN e_l.addr_type IN ('PointAddress', 'StreetAddress', 'StreetInd', 'StreetName') THEN 1
                    ELSE 0
                END) AS calls_loc_m,
            sum(
                CASE
                    WHEN e_l.addr_type IN ('PointAddress', 'StreetAddress', 'StreetInd', 'StreetName', 'Postal') THEN 1
                    ELSE 0
                END) AS calls_loc_l
           FROM ems.basicincident ems LEFT JOIN e_l USING (state, fdid, inc_date, inc_no, exp_no)
		   WHERE ems.inc_type LIKE '4%'
          GROUP BY ems.state, ems.fdid, ("substring"(ems.inc_date, 5, 4)::integer)
        )
 SELECT
        CASE
            WHEN t.state IS NULL THEN e.state
            ELSE t.state
        END AS state,
        CASE
            WHEN t.fdid IS NULL THEN e.fdid
            ELSE t.fdid
        END AS fdid,
        CASE
            WHEN t.year IS NULL THEN e.year
            ELSE t.year
        END AS year,
    t.incidents,
    t.incidents_loc,
    t.v5_incidents,
    t.v5_incidents_loc,
    t.fires,
    t.fires_loc,
    t.mod_fires,
    t.mod_fires_loc,
    t.struc_fires,
    t.struc_fires_loc,
    t.res_fires,
    t.res_fires_loc,
    t.lr_fires,
    t.lr_fires_loc,
    t.injuries,
    t.injuries_loc,
    t.deaths,
    t.deaths_loc,
    e.calls,
    e.calls_loc_s,
    e.calls_loc_m,
    e.calls_loc_l
   FROM t
     FULL JOIN e ON t.state = e.state AND t.fdid = e.fdid AND t.year = e.year;

--ALTER TABLE nist.dept_incidents
--  OWNER TO firecares;
GRANT ALL ON TABLE nist.dept_incidents TO sgilbert;
GRANT SELECT ON TABLE nist.dept_incidents TO firecares;
COMMENT ON MATERIALIZED VIEW nist.dept_incidents
  IS 'Its purpose is to provide the data needed to correct for geolocation errors.
Note this view needs to be refreshed whenever new NFIRS data is added or when records 
are geolocated. (REFRESH MATERIALIZED VIEW nist.dept_incidents;)

This version incorporates ems calls. For now, the geolocations for ems calls are split
between multiple tables. Thus, I build the e_l subquery which UNIONs those tables.

I suspect (but dont know) that the 2014 and 2015 geolocated tables are overlapping. Since
the UNION term only returns DISTINCT rows, I dont have to worry about it here.

This query, as written, will take a long time to run.

When this query is finalized, the clause altering ownership will need to be uncommented.';

