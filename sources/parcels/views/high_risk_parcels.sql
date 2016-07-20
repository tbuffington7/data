CREATE MATERIALIZED VIEW high_risk_parcels as (
    WITH t AS (
        SELECT *
        FROM parcels, high_risk_structures
        WHERE parcels.wkb_geometry && high_risk_structures.geom
    )
    SELECT parcel_id, type, data
    FROM t WHERE ST_INTERSECTS(wkb_geometry, geom));

CREATE INDEX parcel_ids_idx ON high_risk_parcels (parcel_id);
