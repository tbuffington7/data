CREATE MATERIALIZED VIEW IF NOT EXISTS public.joint_civiliancasualty AS
    SELECT * FROM public.civiliancasualty UNION ALL
    SELECT * FROM department_nfirs.civiliancasualty;
