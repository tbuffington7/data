CREATE OR REPLACE VIEW usgs.hospitals AS
(SELECT a.ogc_fid,
  a.geom,
  a.permanent_identifier,
  a.source_featureid,
  a.source_datasetid,
  a.source_datadesc,
  a.source_originator,
  a.data_security,
  a.distribution_policy,
  a.loaddate,
  a.ftype,
  a.fcode,
  a.name,
  a.islandmark,
  a.pointlocationtype,
  a.admintype,
  a.addressbuildingname,
  a.address,
  a.city,
  a.state,
  a.zipcode,
  a.gnis_id,
  a.foot_id,
  a.complex_id,
  a.globalid,
  b.name AS fcode_name
 FROM usgs.structures a
   JOIN usgs.fcode_choices b ON a.fcode = b.id
WHERE b.name::text ~~ '%Hospital%'::text
  OR b.name::text = 'Health or Medical Facility'::text);

