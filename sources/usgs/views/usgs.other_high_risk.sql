CREATE OR REPLACE VIEW usgs.other_high_risk AS
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
  WHERE b.name::text = ANY (ARRAY[
    'Grain Elevator'::character varying,
    'Hazardous Materials Facility'::character varying,
    'Mine'::character varying,
    'Natural Gas Facility'::character varying,
    'Nuclear Fuel Plant'::character varying,
    'Nuclear Research Facility'::character varying,
    'Oil / Gas Facility'::character varying,
    'Oil / Gas Refinery'::character varying,
    'Oil / Gas Storage Facility / Tank Farm'::character varying,
    'Prison / Correctional Facility'::character varying,
    'Psychiatric Facility'::character varying]::text[]));