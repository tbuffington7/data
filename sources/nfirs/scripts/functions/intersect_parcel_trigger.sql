CREATE OR REPLACE FUNCTION intersect_parcel_trigger() RETURNS trigger AS $$
BEGIN
  NEW.parcel_id := get_intersecting_parcel(NEW.geom);
  RETURN NEW;
END;
$$ LANGUAGE 'plpgsql'
