CREATE OR REPLACE function department_for_block_group(varchar) returns integer AS
$$
select firecares_id from (select b.id as firecares_id, fdid, state, count(*), st_distance(a.geom, b.geom) as dis, name
from incidentaddress a
left join tyler.firestation_firedepartment b using (state, fdid)
where bkgpidfp10=$1
group by b.id, fdid, name,  state, dis
order by count desc, dis asc
limit 1) as departments
$$
LANGUAGE SQL;
