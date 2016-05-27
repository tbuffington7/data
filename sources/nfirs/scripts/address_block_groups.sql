
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


CREATE OR REPLACE FUNCTION intersect_census_data_trigger() RETURNS TRIGGER AS $$
 DECLARE
    intersects record;
 BEGIN
     SELECT INTO intersects (intersect_census_data(NEW)).*;
     NEW.bkgpidfp10=intersects.bkgpidfp10;
     NEW.bkgpidfp00=intersects.bkgpidfp00;
     RETURN NEW;
 END;
$$ LANGUAGE plpgsql;



CREATE TRIGGER incident_address_census_update_trigger BEFORE UPDATE OF geom ON incidentaddress FOR EACH ROW EXECUTE PROCEDURE intersect_census_data_trigger();
CREATE TRIGGER incident_address_census_insert_trigger BEFORE INSERT ON incidentaddress FOR EACH ROW EXECUTE PROCEDURE intersect_census_data_trigger();

--update incidentaddress set bkgpidfp10=(intersect_census_data(incidentaddress.*)).bkgpidfp10;