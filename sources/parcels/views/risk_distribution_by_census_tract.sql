CREATE MATERIALIZED VIEW risk_distribution_by_census_tract AS(
WITH risk_distribution as (select initcap(COALESCE(p.risk_category, u.risk_category)) as risk_category,
                                 p.state_code,
                                 substring(p.census_tr,0, 7) as tract,
                                 count(*) as num
                           from parcels p
                           left join "LUSE_swg" u on p.land_use=u."Code"
                           group by u.risk_category, p.risk_category, state_code, tract)
select state_code, tract,
  (select sum(num) from risk_distribution a where risk_category='High' and a.state_code=state_code and b.tract=tract) as high,
  (select sum(num) from risk_distribution a where risk_category='Medium' and a.state_code=state_code and b.tract=tract) as medium,
  (select sum(num) from risk_distribution a where risk_category='Low' and a.state_code=state_code and b.tract=tract) as low
from risk_distribution b
group by state_code, tract);
