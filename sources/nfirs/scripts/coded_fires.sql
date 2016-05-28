/*

# Create the coded_fires intermediate table

* This query collects relevant NFIRS data for each fire into a single table this query returns  all fires, not residential fires.
* Note this view needs to be refreshed whenever new NFIRS data is added or
  when records are geolocated. (REFRESH MATERIALIZED VIEW nist.dept_incidents;)


*/

CREATE MATERIALIZED VIEW nist.coded_fires AS
  WITH t AS (
    SELECT b.state, b.fdid, b.inc_date, b.inc_no, b.exp_no, b.version::NUMERIC(4,1) AS version,
    b.dept_sta, b.inc_type, b.add_wild, b.aid, b.alarm, b.arrival, b.inc_cont, b.lu_clear, 
    b.shift, b.alarms, b.district, b.act_tak1, b.act_tak2, b.act_tak3, b.app_mod, b.sup_app,
    b.ems_app, b.oth_app, b.sup_per, b.ems_per, b.oth_per, b.resou_aid, b.prop_loss, b.cont_loss, 
    b.prop_val, b.cont_val, b.ff_death, b.oth_death, b.ff_inj, b.oth_inj, b.det_alert, b.haz_rel, 
    b.mixed_use, b.prop_use, b.census, a.loc_type, a.num_mile, a.street_pre, a.streetname, a.streettype,
    a.streetsuf, a.apt_no, a.city, a.state_id, a.zip5, a.zip4, a.x_street, b.alarm AS alarm_time,
    b.arrival AS arrival_time, b.arrival-b.alarm AS travel_time, SUBSTRING(a.bkgpidfp00, 0, 11) AS tr00_fid, SUBSTRING(a.bkgpidfp10, 0, 11) AS tr10_fid, a.geom
    FROM basicincident b LEFT JOIN incidentaddress a USING (state, fdid, inc_date, inc_no, exp_no)
    WHERE b.aid NOT IN ( '3' , '4') and a.geom is not NULL
)
  SELECT t.state, t.fdid, t.inc_date, t.inc_no, t.exp_no, extract('year' from t.inc_date) AS YEAR, t.version,
  CASE
    WHEN ( EXTRACT('year' FROM t.inc_date) > 2001 AND t.inc_type IN ('111','120','121','122','123') OR EXTRACT('year' FROM t.inc_date) > 2001 AND EXTRACT('year' FROM t.inc_date) < 2008 AND t.inc_type = '112')
      AND f.struc_type IN ( '1', '2' ) OR ( t.inc_type IN( '113', '114', '115', '116', '117', '118' ) OR t.inc_type = '110' AND EXTRACT('year' FROM t.inc_date) < 2009)
      AND ( f.struc_type IN( '1', '2' ) OR f.struc_type IS NULL ) THEN 'Y'::text
    ELSE 'N'::text
  END AS struc,
  CASE
    WHEN t.prop_use = '419' OR t.prop_use LIKE '9%' THEN 'Low Risk'
    WHEN t.prop_use NOT IN ( '419', '644', '645' ) AND substring(t.prop_use, 1, 1) IN ( '4', '5', '6', '7', '8' ) AND
      ( f.bldg_above IS NULL OR f.bldg_above::INTEGER < 7 ) THEN 'Med Risk'
    ELSE 'High Risk'
  END AS risk,
  t.inc_type, f.not_res, f.fire_sprd, t.prop_use,
  f.struc_type, f.struc_stat, f.bldg_above, t.alarm_time, t.arrival_time,
  t.travel_time, t.ff_death, t.oth_death, t.ff_inj, t.oth_inj,
  '14000US' || t.tr00_fid AS geoid_00,
  '14000US' || t.tr10_fid AS geoid,
   t.geom
  FROM t
  LEFT JOIN fireincident f USING (state, fdid, inc_date, inc_no, exp_no)
  WHERE t.inc_type LIKE '1%' OR f.state IS NOT NULL;

create index coded_fires_idx_year_version_incident ON nist.coded_fires (year, version, inc_type);
WHERE cf.year > 2006 AND cf.year < 2014 AND cf.version = 5.0 AND NOT ( cf.inc_type = '112' AND cf.year > 2007 )