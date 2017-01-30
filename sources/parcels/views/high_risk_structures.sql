CREATE MATERIALIZED VIEW high_risk_structures AS
(
select geom, 'school' as type, row_to_json(row)::jsonb as data from (select * from usgs.schools) row
UNION
select geom, 'hospital' as type, row_to_json(row)::jsonb as data from (select * from usgs.hospitals) row
UNION
select geom, 'high rise' as type, row_to_json(row)::jsonb as data from (select * from emporis.buildings where floors_overground > 7) row
UNION
select geom, 'public assembly' as type, row_to_json(row)::jsonb as data from (select * from usgs.public_assembly) row
UNION
select geom, 'other' as type, row_to_json(row)::jsonb as data from (select * from usgs.other_high_risk) row
UNION
select geom, 'explosive plant' as type, row_to_json(row)::jsonb as data from (select * from eia.biodiesel_plants) row
UNION
select geom, 'explosive plant' as type, row_to_json(row)::jsonb as data from (select * from eia.ethanol_plants) row
UNION
select geom, 'explosive plant' as type, row_to_json(row)::jsonb as data from (select * from eia.natural_gas_processing_plants) row
UNION
select geom, 'refinery' as type, row_to_json(row)::jsonb as data from (select * from eia.petroleum_refineries) row
);

create index high_risk_structure_gist on high_risk_structures USING gist(geom);
