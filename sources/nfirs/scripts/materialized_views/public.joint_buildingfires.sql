CREATE MATERIALIZED VIEW IF NOT EXISTS public.joint_buildingfires AS
    SELECT * FROM public.buildingfires UNION ALL
    SELECT * FROM department_nfirs.buildingfires;
