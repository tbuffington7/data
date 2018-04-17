CREATE MATERIALIZED VIEW nist.tract_years AS 
 WITH t AS (
         SELECT '14000US'::text || "substring"(incidentaddress.bkgpidfp10::text, 1, 11) AS tr10_fid,
            date_part('year'::text, incidentaddress.inc_date) AS year,
            incidentaddress.fdid,
            count(*) AS n
           FROM incidentaddress
          GROUP BY ('14000US'::text || "substring"(incidentaddress.bkgpidfp10::text, 1, 11)), (date_part('year'::text, incidentaddress.inc_date)), incidentaddress.fdid
        ), u AS (
         SELECT t.tr10_fid,
            t.year,
            max(t.n) AS n_max
           FROM t
          GROUP BY t.tr10_fid, t.year
        ), v AS (
         SELECT u.tr10_fid,
            u.year,
            max(t.fdid) as fdid,
            t.n
           FROM u
             JOIN t ON u.tr10_fid = t.tr10_fid AND u.year = t.year AND u.n_max = t.n
        ), y AS (
         SELECT generate_series(2005, 2018) AS year
        ), ty AS (
         SELECT DISTINCT acs.geoid,
            y.year,
            substring(acs.geoid, 8, 2) AS state
           FROM nist.acs2010 acs,
            y
        )
 SELECT ty.geoid AS tr10_fid,
    ty.year,
    s.state_fipscode,
    s.state_abbreviation AS state,
    s.region,
    v.fdid,
    v.n,
    firecares.id AS fc_dept_id
   FROM ty
     LEFT JOIN v ON ty.geoid = v.tr10_fid AND ty.year::double precision = v.year
     LEFT JOIN usgs_stateorterritoryhigh s ON ty.state = s.state_fipscode::text
     LEFT JOIN firestation_firedepartment firecares ON s.state_abbreviation::text = firecares.state::text AND v.fdid::text = firecares.fdid::text
WITH DATA;

COMMENT ON nist.tract_years IS
'An intermediate table that is the Cartesian product of tracts and years of the study.
It also includes some additional information for the tracts, allowing me to reduce the
number of tables referenced. There are some cases where this algorithm returns multiple 
departments to a tract. The v subquery above resolves those ties by picking the first 
fdid (alphabetically) as the owner of the tract.'

ALTER TABLE nist.tract_years
  OWNER TO firecares;
GRANT ALL ON TABLE nist.tract_years TO firecares;
GRANT ALL ON TABLE nist.tract_years TO sgilbert;
