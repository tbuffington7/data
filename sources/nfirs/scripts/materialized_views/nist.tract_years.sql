-- An intermediate table that is the Cartesian product of tracts and years of the study.
-- It also includes some additional information for the tracts, allowing me to reduce the
--  number of tables referenced.

CREATE MATERIALIZED VIEW nist.tract_years AS
(select state, fdid,
  '14000US' || SUBSTRING(bkgpidfp00, 0, 11) AS tr00_fid,
  '14000US' || SUBSTRING(bkgpidfp10, 0, 11) as tr10_fid,
  fd.id as fc_dept_id,
  extract('year' from inc_date) as year, fd.region
from incidentaddress ia left join firestation_firedepartment fd using (state, fdid)
where bkgpidfp00 is not null
group by state, fdid, bkgpidfp00,bkgpidfp10, fc_dept_id, year, fd.region);

