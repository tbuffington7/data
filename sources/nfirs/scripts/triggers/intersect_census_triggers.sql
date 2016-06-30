CREATE TRIGGER incident_address_census_update_trigger
  BEFORE UPDATE OF geom ON incidentaddress
  FOR EACH ROW EXECUTE PROCEDURE intersect_census_data_trigger();

CREATE TRIGGER incident_address_census_insert_trigger
  BEFORE INSERT ON incidentaddress
  FOR EACH ROW EXECUTE PROCEDURE intersect_census_data_trigger();
