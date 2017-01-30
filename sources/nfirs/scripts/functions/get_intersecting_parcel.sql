CREATE OR REPLACE FUNCTION get_intersecting_parcel(geometry)
RETURNS integer AS $$
 SELECT parcels.parcel_id::int
      FROM parcel_risk_category_local parcels
      WHERE ST_Intersects($1, parcels.wkb_geometry)
       ORDER BY st_distance(st_centroid($1), parcels.wkb_geometry)
      limit 1
$$ LANGUAGE SQL;
