CREATE MATERIALIZED VIEW nist.consolidated_hr_parcels_swg AS 
 WITH p0 AS (
         SELECT p.state_code,
            p.cnty_code,
            p.risk_class,
            p.res_other,
            st_dump(st_union(st_makevalid(p.wkb_geometry))) AS g
           FROM nist.hr_parcels_local p
          GROUP BY p.state_code, p.cnty_code, p.risk_class, p.res_other
        ), p1 AS (
         SELECT p0.state_code,
            p0.cnty_code,
            p0.risk_class,
            p0.res_other,
            (p0.g).path[1] AS path,
            (p0.g).geom AS geom
           FROM p0
        )
 SELECT min(h.parcel_id) AS p_id,
    count(h.*) AS n_parcels,
    p1.state_code,
    p1.cnty_code,
    p1.risk_class,
    p1.res_other,
    p1.path,
    p1.geom
   FROM p1,
    nist.hr_parcels_local h
  WHERE p1.geom && h.wkb_geometry AND p1.state_code::text = h.state_code::text AND p1.risk_class = h.risk_class AND p1.res_other = h.res_other
  GROUP BY p1.state_code, p1.cnty_code, p1.risk_class, p1.res_other, p1.path, p1.geom
WITH DATA;

ALTER TABLE nist.consolidated_hr_parcels_swg
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.consolidated_hr_parcels_swg TO sgilbert;
GRANT SELECT ON TABLE nist.consolidated_hr_parcels_swg TO firecares;
COMMENT ON MATERIALIZED VIEW nist.consolidated_hr_parcels_swg
  IS 'Columns have the following types:
  p_id        double precision
  n_parcels   integer
  state_code  varchar
  cnty_code   varchar
  risk_class  text
  res_other   text
  path        integer
  geom        geometry( POLYGON, 4326 )

This is identical to the table with the same name in the Parcels database. 
It is recreated here so that I can build an index on it--something I cannot
do on a foreign table.

The parcels data set has many cases where multiple parcels are in the same
location. These are probably multiple units (like condos) part of a single 
structure. Since  fires are included in the analysis based on whether they 
intersect with a parcel bounding box, I end up with multiple copies (sometimes
hundreds or thousands) of the same fire in the analysis in those cases. 
Similarly, when it comes to estimating the number of fires or casualties, I 
end up with multiple estimates in the same location. This has caused problems
overinflating the number of high-risk fires for some localities.

The basic approach is to merge all intersecting high-risk parcels with the same
risk class into a single object. Then pick a parcel identifier to associate with
it.

Subqueries are described as follows:

p0: This UNIONs all parcels (broken up by risk class, residential status, and 
county--the latter mainly to keep the UNIONed object from getting too big) into
a single object and then breaks these very large objects (consisting of many
individual polygons) apart into the individual constituent polygons.

p1: Since the ST_DUMP function returns a set, this converts the members of the
returned set into fields in the table.

Main Query: This JOINS all relevant parcels whose bounding box intersects the 
bounding box of the consolidated parcel, and picks one essentially arbitarily 
to represent the consolidated parcel. Note that since the bounding box is 
larger than the actual geometry, it is possible that a parcel will be chosen
to represent a consolidated parcel that is not actually part of the consolidated
parcel.';

-- Index: nist.ndx_consolidated_geom

CREATE INDEX ndx_consolidated_geom
  ON nist.consolidated_hr_parcels_swg
  USING gist
  (geom);
