CREATE OR REPLACE FUNCTION intersect_census_data(incidentaddress)
RETURNS TABLE(bkgpidfp00 varchar, bkgpidfp10 varchar) AS $$
 SELECT ROW(
   (SELECT bg.bkgpidfp00
      FROM census_block_groups_2000 bg
      WHERE ST_Intersects($1.geom, bg.geom)),
   (SELECT b.geoid10
      FROM census_block_groups_2010 b
      WHERE ST_Intersects($1.geom, b.geom))
 )
$$ LANGUAGE SQL;
