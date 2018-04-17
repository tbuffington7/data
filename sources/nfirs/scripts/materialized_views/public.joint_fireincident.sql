CREATE MATERIALIZED VIEW IF NOT EXISTS public.joint_fireincident AS
    SELECT * FROM public.fireincident UNION ALL
    SELECT * FROM department_nfirs.fireincident;
