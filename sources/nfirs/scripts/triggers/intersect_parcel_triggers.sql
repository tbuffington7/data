CREATE TRIGGER intersect_parcel_update_trigger
  BEFORE UPDATE OF geom ON incidentaddress
  FOR EACH ROW EXECUTE PROCEDURE intersect_parcel_trigger();

CREATE TRIGGER intersect_parcel_insert_trigger
  BEFORE INSERT ON incidentaddress
  FOR EACH ROW EXECUTE PROCEDURE intersect_parcel_trigger();
