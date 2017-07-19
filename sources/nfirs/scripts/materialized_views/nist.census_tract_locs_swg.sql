CREATE MATERIALIZED VIEW nist.census_tract_locs_swg AS 
 SELECT (('14000US'::text || bg.statefp10::text) || bg.countyfp10::text) || bg.tractce10::text AS geoid,
    bg.statefp10,
    bg.countyfp10,
    st_union(st_makevalid(bg.geom)) AS geom
   FROM census_block_groups_2010 bg
  GROUP BY (('14000US'::text || bg.statefp10::text) || bg.countyfp10::text) || bg.tractce10::text, bg.statefp10, bg.countyfp10
WITH DATA;

ALTER TABLE nist.census_tract_locs_swg
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.census_tract_locs_swg TO sgilbert;
GRANT SELECT ON TABLE nist.census_tract_locs_swg TO firecares;
COMMENT ON MATERIALIZED VIEW nist.census_tract_locs_swg
  IS 'I neeed a location polygon for tracts, but what I have are location polygons
for block groups. This creates polygons for tracts by UNIONing the constituent
block group polygons.';

-- Index: nist.ndx_tracts_geom

-- DROP INDEX nist.ndx_tracts_geom;

CREATE INDEX ndx_tracts_geom
  ON nist.census_tract_locs_swg
  USING gist
  (geom);
