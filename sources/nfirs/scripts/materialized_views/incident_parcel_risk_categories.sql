/*

Note: incident_parcel_risk_categories is a foreign table using FDW
Since this is AWS, its IP address may change and we need to update the
route in the Security Group since RDS does not allow us to whitelist the
 */

CREATE TABLE parcel_risk_category_local as (SELECT * FROM incident_parcel_risk_categories);

CREATE index parcel_risk_category_local_geom_gix
  ON parcel_risk_category_local using gist(wkb_geometry);

CREATE index parcel_risk_category_local_risk_category_partial
  ON parcel_risk_category_local (risk_category) where (risk_category is not null);

CREATE MATERIALIZED VIEW incident_address_risk as (SELECT
  *
FROM (
  SELECT state, fdid,
    inc_date,
    inc_no,
    exp_no,
    b.parcel_id,
    b.risk_category,
    ROW_NUMBER() OVER (PARTITION BY state, fdid, inc_date, inc_no, exp_no, risk_category ORDER BY st_distance(st_centroid(b.wkb_geometry), a.geom)) AS r
  FROM incidentaddress a
     inner join parcel_risk_category_local b on a.geom && b.wkb_geometry
     where b.risk_category is not null
     ) x
WHERE
  x.r = 1
);

create index incident_address_risk_state_fdid_inc_date_inc_no_exp_no_idx
  on incident_address_risk using btree(state, fdid, inc_date, inc_no, exp_no);
