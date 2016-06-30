CREATE OR REPLACE FUNCTION refresh_nist_materialized_views()
RETURNS void LANGUAGE plpgsql
AS $$
BEGIN
    REFRESH MATERIALIZED VIEW nist.dept_incidents;
    REFRESH MATERIALIZED VIEW nist.coded_fires;
    REFRESH MATERIALIZED VIEW nist.casualties_fire;
    REFRESH MATERIALIZED VIEW nist.tract_years;
    REFRESH MATERIALIZED VIEW nist.final_query;
END $$;
