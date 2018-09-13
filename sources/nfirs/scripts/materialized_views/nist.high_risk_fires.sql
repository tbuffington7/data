CREATE MATERIALIZED VIEW nist.high_risk_fires AS 
 WITH f AS (
         SELECT hr_parcel_fires.year,
            hr_parcel_fires.parcel_id,
            hr_parcel_fires.geoid,
            hr_parcel_fires.geoid_source,
            hr_parcel_fires.res_corelogic,
            hr_parcel_fires.res_other,
            hr_parcel_fires.bld_units,
            hr_parcel_fires.hr_floors,
            hr_parcel_fires.eff_yr,
            hr_parcel_fires.risk_class,
            sum(
                CASE
                    WHEN hr_parcel_fires.struc = 'Y'::text AND hr_parcel_fires.inc_type::text ~~ '11%'::text THEN 1
                    ELSE 0
                END) AS fires,
            sum(
                CASE
                    WHEN hr_parcel_fires.struc = 'Y'::text AND hr_parcel_fires.inc_type::text ~~ '11%'::text AND (hr_parcel_fires.fire_sprd IS NOT NULL OR (hr_parcel_fires.inc_type::text = ANY (ARRAY['113'::text, '114'::text, '115'::text, '116'::text, '117'::text, '118'::text]))) THEN 1
                    ELSE 0
                END) AS size_1,
            sum(
                CASE
                    WHEN hr_parcel_fires.struc = 'Y'::text AND hr_parcel_fires.inc_type::text ~~ '11%'::text AND (hr_parcel_fires.fire_sprd::text = ANY (ARRAY['3'::text, '4'::text, '5'::text])) THEN 1
                    ELSE 0
                END) AS size_2,
            sum(
                CASE
                    WHEN hr_parcel_fires.struc = 'Y'::text AND hr_parcel_fires.inc_type::text ~~ '11%'::text AND hr_parcel_fires.fire_sprd::text = '5'::text THEN 1
                    ELSE 0
                END) AS size_3,
            sum(
                CASE
                    WHEN hr_parcel_fires.struc = 'Y'::text AND hr_parcel_fires.inc_type::text ~~ '11%'::text THEN hr_parcel_fires.ff_death + hr_parcel_fires.oth_death
                    ELSE 0
                END) AS deaths,
            sum(
                CASE
                    WHEN hr_parcel_fires.struc = 'Y'::text AND hr_parcel_fires.inc_type::text ~~ '11%'::text THEN hr_parcel_fires.ff_inj + hr_parcel_fires.oth_inj
                    ELSE 0
                END) AS injuries
           FROM nist.hr_parcel_fires
          GROUP BY hr_parcel_fires.year, hr_parcel_fires.parcel_id, hr_parcel_fires.geoid, hr_parcel_fires.geoid_source, hr_parcel_fires.res_corelogic, hr_parcel_fires.res_other, hr_parcel_fires.bld_units, hr_parcel_fires.hr_floors, hr_parcel_fires.eff_yr, hr_parcel_fires.risk_class
        ), d AS (
         SELECT i.year,
            d_1.id AS fd_id,
            'size_'::text || d_1.population_class::text AS fd_size,
            sum(i.incidents) AS incidents,
            sum(i.incidents_loc) AS located,
            sum(i.fires) AS dept_fires
           FROM nist.dept_incidents i
             JOIN firestation_firedepartment d_1 USING (state, fdid)
          WHERE i.year > 2006::double precision
          GROUP BY i.year, d_1.id, d_1.population_class
        )
 SELECT f.year,
    f.parcel_id,
    f.geoid,
    f.geoid_source,
    t.region,
    t.state,
    t.fc_dept_id AS fd_id,
    d.fd_size,
    f.res_corelogic,
    f.res_other,
    f.bld_units,
    f.hr_floors,
    f.eff_yr,
    f.risk_class,
    d.incidents AS dept_incidents,
    d.dept_fires,
        CASE
            WHEN d.incidents > 0::numeric THEN (d.located / d.incidents)::double precision
            WHEN d.incidents = 0::numeric THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS f_located,
    f.fires,
    f.size_1,
    f.size_2,
    f.size_3,
    f.deaths,
    f.injuries,
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
    sm.adult_smoke AS smoke_st,
    sc.smoking_pct AS smoke_cty
   FROM f
     LEFT JOIN nist.tract_years t ON f.year = t.year AND f.geoid = t.tr10_fid
     LEFT JOIN d ON t.year::double precision = d.year AND t.fc_dept_id = d.fd_id
     LEFT JOIN nist.svi2010 svi ON f.geoid = ('14000US'::text || lpad(svi.fips::text, 11, '0'))
     LEFT JOIN nist.acs_est_new acs ON f.geoid = acs.geoid AND
    CASE
      WHEN tr.year < 2008 THEN 2008
      WHEN tr.year > (select max(year) from nist.acs_est_new) - 2 THEN (select max(year) from nist.acs_est_new) - 2
      ELSE tr.year
    END::double precision = (acs.year - 2::double precision)
     LEFT JOIN nist.sins sm ON t.state::text = sm.postal_code AND sm.year = 2010
     LEFT JOIN nist.sins_county sc ON "substring"(f.geoid, 8, 5) = sc.fips
WITH DATA;

ALTER TABLE nist.high_risk_fires
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.high_risk_fires TO sgilbert;
GRANT SELECT ON TABLE nist.high_risk_fires TO firecares;
COMMENT ON MATERIALIZED VIEW nist.high_risk_fires
  IS 'This summarized the fire information for each high-risk parcel by year. It also
includes data that will be used to estimate the model. Data is from NFIRS 
(indirectly through dept_incidents), the ACS, CoreLogic, and several other 
sources.';
