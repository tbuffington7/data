CREATE MATERIALIZED VIEW nist.county_clustering AS
WITH t1 AS (
SELECT DISTINCT
  substring(geoid from 8 for 5) as geoid, years_lost,  poor_health,     days_pr_hlth,  days_pr_mntl,   low_birthwt, 
  csmoking,        obesity,           food_ndx, lpa,   exercise_place,  binge, dui,    stds,           teen_births,   access2, 
  physicians,      dentists,          shrinks,         wrong_hosp,      diabetic_scrn, mammography,    high_school,   college, 
  child_pov,       inequality,        child_sngl_prnt, social,          violent,       injury_dths,    pm10, npdes,   house_probs, 
  drive_alone,     long_commute,      early_mortality, child_mortality, infant_death,  phys_distress,  mntl_distress, 
  diabetes,        hiv,               food_insecurity, no_healthy_food, drug_overdose, drug_overdose2, mv_deaths,     lack_sleep, 
  uninsured_adult, uninsured_child,   hlth_cost,       nurses,          free_lunch,    segregation1,   segregation2,  homicide, rural
FROM nist.ems_table_cnty
), t2 AS (
SELECT
  substring(geoid from 8 for 5) as geoid, sum(pop) AS pop,    sum(black) AS black,                sum(amer_es) AS amer_es,       sum(other) AS other, 
  sum(hispanic) AS hispanic,   sum(males) AS males,           sum(age_under5) AS age_under5,      sum(age_5_9) AS age_5_9,       sum(age_10_14) AS age_10_14, 
  sum(age_15_19) AS age_15_19, sum(age_20_24) AS age_20_24,   sum(age_25_34) AS age_25_34,        sum(age_35_44) AS age_35_44, 
  sum(age_45_54) AS age_45_54, sum(age_55_64) AS age_55_64,   sum(age_65_74) AS age_65_74,        sum(age_75_84) AS age_75_84, 
  sum(age_85_up) AS age_85_up, sum(hse_units) AS hse_units,   sum(vacant) AS vacant,              sum(renter_occ) AS renter_occ, sum(crowded) AS crowded, 
  sum(sfr) AS sfr,             sum(units_10) AS units_10,     sum(mh) AS mh, sum(older) AS older, sum(married) AS married,       sum(unemployed) AS unemployed, 
  sum(nilf) AS nilf,           sum(fuel_gas) AS fuel_gas,     sum(fuel_tank) AS fuel_tank,        sum(fuel_oil) AS fuel_oil,     sum(fuel_coal) AS fuel_coal, 
  sum(fuel_wood) AS fuel_wood, sum(fuel_solar) AS fuel_solar, sum(fuel_other) AS fuel_other,      sum(fuel_none) AS fuel_none
FROM nist.ems_table_cnty
GROUP BY substring(geoid from 8 for 5)
), t3 AS (
SELECT
  substring(geoid from 8 for 5) as geoid, 
  sum(hse_units * ave_hh_sz) AS ave_hh_size,
  sum(CASE 
        WHEN inc_hh = 'null' THEN Null
        ELSE hse_units * inc_hh::double precision
      END) AS inc_hh
FROM nist.ems_table_cnty
GROUP BY substring(geoid from 8 for 5)
), c AS (
SELECT
  geoid,
  st_centroid(wkb_geometry) as geom,
  st_x(st_centroid(wkb_geometry)) AS x,
  st_y(st_centroid(wkb_geometry)) AS y
FROM us_counties
)
SELECT 
  c.geoid,
  c.x,
  c.y,
  pop,       black,     amer_es,    other,     hispanic,  males,     age_under5, age_5_9,    age_10_14, 
  age_15_19, age_20_24, age_25_34,  age_35_44, age_45_54, age_55_64, age_65_74,  age_75_84,  age_85_up, 
  hse_units, vacant,    renter_occ, crowded,   sfr,       units_10,  mh, older,  married,    unemployed, 
  nilf,      fuel_gas,  fuel_tank,  fuel_oil,  fuel_coal, fuel_wood, fuel_solar, fuel_other, fuel_none
  years_lost,      poor_health,     days_pr_hlth,    days_pr_mntl,    low_birthwt, 
  csmoking,        obesity,         food_ndx, lpa,   exercise_place,  binge, dui,    stds,           teen_births,   access2, 
  physicians,      dentists,        shrinks,         wrong_hosp,      diabetic_scrn, mammography,    high_school,   college, 
  child_pov,       inequality,      child_sngl_prnt, social,          violent,       injury_dths,    pm10, npdes,   house_probs, 
  drive_alone,     long_commute,    early_mortality, child_mortality, infant_death,  phys_distress,  mntl_distress, 
  diabetes,        hiv,             food_insecurity, no_healthy_food, drug_overdose, drug_overdose2, mv_deaths,     lack_sleep, 
  uninsured_adult, uninsured_child, hlth_cost,       nurses,          free_lunch,    segregation1,   segregation2,  homicide, rural,
  t3.ave_hh_size / t2.hse_units AS ave_hh_size,
  t3.inc_hh      / t2.hse_units AS inc_hh  
FROM c LEFT JOIN t1 USING (geoid)
       LEFT JOIN t2 USING (geoid)
       LEFT JOIN t3 USING (geoid);

ALTER TABLE nist.county_clustering
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.county_clustering TO sgilbert;
GRANT SELECT ON TABLE nist.county_clustering TO firecares;

COMMENT ON MATERIALIZED VIEW nist.county_clustering
  IS 'This summarizes the EMS (county) predictors by rolling them up to the county level.
The information in subquery t1 is largely from www.countyhealthrankings.org and is
already at the county level. So they simply need to be collected. The information in subquery
t2 is at the census tract level, and needs to be summed up to the county level. The information
in subquery t3 is averaged per household per census tract. To roll it up to the county level 
requires a weighted average by number of households. The terms x and y are approximate centroids
on the county, and are used to provide a rough regionalization to the cluster algorithm.

Eventually ownership needs to be handed off to FireCARES.';
