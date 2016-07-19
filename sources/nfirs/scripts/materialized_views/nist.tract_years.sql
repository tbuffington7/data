-- An intermediate table that is the Cartesian product of tracts and years of the study.
-- It also includes some additional information for the tracts, allowing me to reduce the
--  number of tables referenced.

CREATE MATERIALIZED VIEW nist.tract_years AS
(WITH t as ( SELECT generate_series( 2000, 2015 ) as year )

SELECT DISTINCT
    ia.state,
    fdid,
    '14000US'::text || "substring"(ia.bkgpidfp00::text, 0, 11) AS tr00_fid,
    '14000US'::text || "substring"(ia.bkgpidfp10::text, 0, 11) AS tr10_fid,
    fd.id AS fc_dept_id,
    fd.region,
    year
FROM incidentaddress ia LEFT JOIN firestation_firedepartment fd USING (state, fdid), t
WHERE ia.bkgpidfp00 IS NOT NULL);

