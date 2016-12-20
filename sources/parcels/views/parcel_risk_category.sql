CREATE MATERIALIZED VIEW parcel_risk_category AS
(select p.parcel_id,
  initcap(COALESCE(p.risk_category, u.risk_category)) as risk_category,
  p.wkb_geometry
from parcels p
left join "LUSE_swg" u on p.land_use=u."Code");

create index parcel_risk_category_local_geom_gix
  ON parcel_risk_category_local using gist(wkb_geometry);

create index parcel_risk_category_parcel_id
  ON parcel_risk_category using btree(parcel_id);



