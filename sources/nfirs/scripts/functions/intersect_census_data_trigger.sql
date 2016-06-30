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