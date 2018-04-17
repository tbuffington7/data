-- Materialized View: nist.ems_table_cnty

CREATE MATERIALIZED VIEW nist.ems_table_cnty AS 
 WITH f AS (
         SELECT substring(cf.inc_date, 5, 4)::integer AS year,
            cf.tr10_fid,
            count(*) AS calls
           FROM ems.basicincident cf
          WHERE substring(cf.inc_date, 5, 4)::integer > 2006 AND
		        inc_type like '3%'
          GROUP BY (substring(cf.inc_date, 5, 4)::integer), cf.tr10_fid
        ), d AS (
         SELECT i.year,
            d_1.id AS fd_id,
            'size_'::text || d_1.population_class AS fd_size,
            sum(i.incidents) AS incidents,
            sum(i.incidents_loc) AS located,
            sum(i.calls) AS calls,
            sum(i.calls_loc_s) AS calls_loc_s,
            sum(i.calls_loc_m) AS calls_loc_m,
            sum(i.calls_loc_l) AS calls_loc_l
           FROM nist.dept_incidents2 i
             JOIN firestation_firedepartment d_1 USING (state, fdid)
          WHERE i.year > 2006
          GROUP BY i.year, d_1.id, d_1.population_class
        )
 SELECT tr.year,
    tr.tr10_fid AS geoid,
    tr.region,
    tr.state,
    tr.fdid,
    tr.fc_dept_id,
    d.fd_size,
    d.calls AS dept_calls,
        CASE
            WHEN d.calls > 0::numeric THEN d.calls_loc_s::double precision / d.calls::double precision
            WHEN d.calls = 0::numeric AND d.calls_loc_s > 0::numeric THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS c_located_s,
        CASE
            WHEN d.calls > 0::numeric THEN d.calls_loc_m::double precision / d.calls::double precision
            WHEN d.calls = 0::numeric AND d.calls_loc_m > 0::numeric THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS c_located_m,
        CASE
            WHEN d.calls > 0::numeric THEN d.calls_loc_l::double precision / d.calls::double precision
            WHEN d.calls = 0::numeric AND d.calls_loc_l > 0::numeric THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS c_located_l,
        CASE
            WHEN acs."B25002_002" > 0 THEN acs."B01001_001"::double precision / acs."B25002_002"::double precision
            WHEN acs."B25002_002" = 0 AND acs."B01001_001" > 0 THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS ave_hh_sz,
    acs."B01001_001" AS pop,
    acs."B02001_003" AS black,
    acs."B02001_004" AS amer_es,
    acs."B02001_005" + acs."B02001_006" + acs."B02001_007" + acs."B02001_008" AS other,
    acs."B03003_003" AS hispanic,
    acs."B01001_002" AS males,
    acs."B01001_003" + acs."B01001_027" AS age_under5,
    acs."B01001_004" + acs."B01001_028" AS age_5_9,
    acs."B01001_005" + acs."B01001_029" AS age_10_14,
    acs."B01001_006" + acs."B01001_007" + acs."B01001_030" + acs."B01001_031" AS age_15_19,
    acs."B01001_008" + acs."B01001_009" + acs."B01001_010" + acs."B01001_032" + acs."B01001_033" + acs."B01001_034" AS age_20_24,
    acs."B01001_011" + acs."B01001_012" + acs."B01001_035" + acs."B01001_036" AS age_25_34,
    acs."B01001_013" + acs."B01001_014" + acs."B01001_037" + acs."B01001_038" AS age_35_44,
    acs."B01001_015" + acs."B01001_016" + acs."B01001_039" + acs."B01001_040" AS age_45_54,
    acs."B01001_017" + acs."B01001_018" + acs."B01001_019" + acs."B01001_041" + acs."B01001_042" + acs."B01001_043" AS age_55_64,
    acs."B01001_020" + acs."B01001_021" + acs."B01001_022" + acs."B01001_044" + acs."B01001_045" + acs."B01001_046" AS age_65_74,
    acs."B01001_023" + acs."B01001_024" + acs."B01001_047" + acs."B01001_048" AS age_75_84,
    acs."B01001_025" + acs."B01001_049" AS age_85_up,
    acs."B25002_001" AS hse_units,
    acs."B25002_003" AS vacant,
    acs."B25014_008" AS renter_occ,
    acs."B25014_005" + acs."B25014_006" + acs."B25014_007" + acs."B25014_011" + acs."B25014_012" + acs."B25014_013" AS crowded,
    acs."B25024_002" + acs."B25024_003" + acs."B25024_004" AS sfr,
    acs."B25024_007" + acs."B25024_008" + acs."B25024_009" AS units_10,
    acs."B25024_010" AS mh,
    acs."B25034_006" + acs."B25034_007" + acs."B25034_008" + acs."B25034_009" + acs."B25034_010" AS older,
    acs."B19013_001" AS inc_hh,
    svi.r_pl_themes AS svi,
    acs."B12001_001" - (acs."B12001_003" + acs."B12001_012") AS married,
    acs."B23025_005" AS unemployed,
    acs."B12001_007" AS nilf,
    acs."B25040_002" AS fuel_gas,
    acs."B25040_003" AS fuel_tank,
    acs."B25040_005" AS fuel_oil,
    acs."B25040_006" AS fuel_coal,
    acs."B25040_007" AS fuel_wood,
    acs."B25040_008" AS fuel_solar,
    acs."B25040_009" AS fuel_other,
    acs."B25040_010" AS fuel_none,
        CASE
            WHEN h.var_058v > 0 THEN h.var_001v::double precision / h.var_058v::double precision
            WHEN h.var_058v = 0 AND h.var_001v > 0 THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS years_lost,
    h.var_002v AS poor_health,
    h.var_003v AS days_pr_hlth,
    h.var_004v AS days_pr_mntl,
    h.var_005v AS low_birthwt,
    h.var_006v AS csmoking,
    h.var_007v AS obesity,
    h.var_008v AS food_ndx,
    h.var_009v AS lpa,
    h.var_010v AS exercise_place,
    h.var_011v AS binge,
    h.var_012v AS dui,
    h.var_013v AS stds,
    h.var_014v AS teen_births,
    h.var_015v AS access2,
        CASE
            WHEN h.var_058v > 0 THEN h.var_016v::double precision / h.var_058v::double precision
            WHEN h.var_058v = 0 AND h.var_016v > 0 THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS physicians,
        CASE
            WHEN h.var_058v > 0 THEN h.var_017v::double precision / h.var_058v::double precision
            WHEN h.var_058v = 0 AND h.var_017v > 0 THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS dentists,
        CASE
            WHEN h.var_058v > 0 THEN h.var_018v::double precision / h.var_058v::double precision
            WHEN h.var_058v = 0 AND h.var_018v > 0 THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS shrinks,
    h.var_019v AS wrong_hosp,
    h.var_020v AS diabetic_scrn,
    h.var_021v AS mammography,
    h.var_022v AS high_school,
    h.var_023v AS college,
    h.var_025v AS child_pov,
    h.var_026v AS inequality,
    h.var_027v AS child_sngl_prnt,
    h.var_028v AS social,
    h.var_029v AS violent,
    h.var_030v AS injury_dths,
    h.var_031v AS pm10,
    h.var_032v AS npdes,
    h.var_033v AS house_probs,
    h.var_034v AS drive_alone,
    h.var_035v AS long_commute,
    h.var_036v AS early_mortality,
    h.var_037v AS child_mortality,
    h.var_038v AS infant_death,
    h.var_039v AS phys_distress,
    h.var_040v AS mntl_distress,
    h.var_041v AS diabetes,
    h.var_042v AS hiv,
    h.var_043v AS food_insecurity,
    h.var_044v AS no_healthy_food,
    h.var_045v AS drug_overdose,
    h.var_046v AS drug_overdose2,
    h.var_047v AS mv_deaths,
    h.var_048v AS lack_sleep,
    h.var_049v AS uninsured_adult,
    h.var_050v AS uninsured_child,
    h.var_051v AS hlth_cost,
        CASE
            WHEN h.var_058v > 0 THEN h.var_052v::double precision / h.var_058v::double precision
            WHEN h.var_058v = 0 AND h.var_052v > 0 THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS nurses,
    h.var_054v AS free_lunch,
    h.var_055v AS segregation1,
    h.var_056v AS segregation2,
    h.var_057v AS homicide,
        CASE
            WHEN h.var_058v > 0 THEN h.var_060v / h.var_058v::double precision
            WHEN h.var_058v = 0 AND h.var_060v > 0::double precision THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS rural
   FROM nist.tract_years tr
     LEFT JOIN f ON tr.tr10_fid::text = f.tr10_fid AND tr.year::double precision = f.year::double precision
     LEFT JOIN d ON tr.fc_dept_id = d.fd_id AND tr.year::double precision = d.year::double precision
     LEFT JOIN nist.svi2010 svi ON tr.tr10_fid::text = ('14000US'::text || lpad(svi.fips::text, 11, '0'::text))
     LEFT JOIN nist.acs_est_new acs ON tr.tr10_fid::text = acs.geoid AND
        CASE
            WHEN tr.year < 2008 THEN 2008
            WHEN tr.year > 2013 THEN 2013
            ELSE tr.year
        END::double precision = (acs.year - 2::double precision)
     LEFT JOIN nist.county_health h ON substring(tr.tr10_fid::text, 8, 5) = (h."STATECODE" || h."COUNTYCODE")
  WHERE tr.year > 2006
WITH DATA;

ALTER TABLE nist.ems_table_cnty
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.ems_table_cnty TO sgilbert;
GRANT SELECT ON TABLE nist.ems_table_cnty TO firecares;

COMMENT ON MATERIALIZED VIEW nist.ems_table_cnty
  IS 'This summarizes the information for ems calls by tract and year. 
It also includes data that will be used to estimate the model. Data is from 
NFIRS (indirectly through dept_incidents), the ACS, CoreLogic, the BRFSS, and 
several other sources.

There are two versions of this table. In this version, the BFRSS data is summarized
to the county level, but covers almost all the county.

Two things need to be dealt with before completing this. 
* The dept_incidents view needs to be updated. 
* Ownership needs to be handed over to firecares.';
