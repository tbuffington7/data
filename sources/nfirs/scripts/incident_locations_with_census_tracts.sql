/*

# Create the incidentaddress_with_census_tracts_and_lat_lons  View#

Generates a view of the "incidentaddresses" table with year 2000 and 2010 census tracts and
a latitude and longitude field.

*/
CREATE OR REPLACE VIEW incidentaddress_with_census_tracts_and_lat_lons AS
SELECT a.*, b.tractce00 AS census_2000_tract_id, c.tractce10 census_2010_tract_id, st_x(a.geom) AS longitude, st_y(a.geom) AS latitude
FROM incidentaddress a
LEFT JOIN census_block_groups_2000 b ON a.bkgpidfp00=b.bkgpidfp00
LEFT JOIN census_block_groups_2010 c on a.bkgpidfp10=c.geoid10;
