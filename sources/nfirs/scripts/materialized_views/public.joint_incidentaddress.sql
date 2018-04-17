CREATE MATERIALIZED VIEW IF NOT EXISTS public.joint_incidentaddress AS
    SELECT state, fdid, inc_date, inc_no, exp_no, geom, parcel_id FROM public.incidentaddress UNION ALL
    SELECT state, fdid, inc_date, inc_no, exp_no, geom, parcel_id FROM department_nfirs.incidentaddress;
