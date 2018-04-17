CREATE MATERIALIZED VIEW IF NOT EXISTS public.joint_ffcasualty AS
    SELECT * FROM public.ffcasualty UNION ALL
    SELECT * FROM department_nfirs.ffcasualty;
