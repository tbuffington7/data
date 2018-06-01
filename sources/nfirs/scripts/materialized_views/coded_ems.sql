CREATE MATERIALIZED VIEW nist.coded_ems AS 
SELECT b.state,
  b.fdid,
  b.inc_date,
  b.inc_no,
  b.exp_no,
  b.sup_app,
  b.sup_per,
  b.ems_app,
  b.ems_per,
  b.oth_app,
  b.oth_per,
  b.ff_death,
  b.oth_death,
  b.ff_inj,
  b.oth_inj,
  b.prop_use,
  a.loc_type,
  c.geoid AS tr10_fid,
  a.geom
FROM ems.basicincident b
  LEFT JOIN ems.incidentaddress a 
    ON b.state::text = a.state::text AND 
	   ltrim(b.fdid::text, ' 0'::text) = ltrim(a.fdid::text, ' 0'::text) AND 
	   b.inc_date = a.inc_date AND
       CASE
         WHEN b.inc_no::text ~ '[ 0-9]*'::text THEN ltrim(b.inc_no::text, ' 0'::text) = ltrim(a.inc_no::text, ' 0'::text)
         ELSE b.inc_no::text = a.inc_no::text
       END AND 
	   b.exp_no = a.exp_no
  LEFT JOIN nist.census_tract_locs_swg c ON st_within(a.geom, c.geom)
WHERE 
  b.inc_type::text ~~ '3%'::text AND 
  b.aid::text NOT IN ('3'::text, '4'::text)
WITH DATA;

ALTER TABLE nist.coded_ems
  OWNER TO firecares;
GRANT ALL ON TABLE nist.coded_ems TO sgilbert;
GRANT ALL ON TABLE nist.coded_ems TO firecares;

COMMENT ON MATERIALIZED VIEW nist.coded_ems
  IS 'This pre-joins the basicincident and incidentaddress tables and specifically 
selects EMS calls. The join is complicated because in geocoding and building the 
incidentaddress table (for years 2012 and 2013), the FDID and INC_DATE fields were 
stripped of leading zeros (at least, it they could be converted to integers). This 
Materialized View means the the very long times required to join those tables only 
have to be incurred once. It also means that if the join conditions change, they 
only have to be corrected in one place.';
