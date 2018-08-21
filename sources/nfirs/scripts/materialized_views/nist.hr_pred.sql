CREATE MATERIALIZED VIEW nist.hr_pred AS 
 WITH f AS (
         SELECT DISTINCT h.parcel_id,
            h.geoid,
            h.geoid_source,
            h.res_corelogic,
            h.res_other,
            h.bld_units,
            h.hr_floors,
            h.eff_yr,
            h.risk_class
           FROM nist.hr_parcel_fires h
        )
 SELECT t.year,
    f.parcel_id,
    f.geoid,
    f.geoid_source,
    t.region,
    t.state,
    t.fc_dept_id AS fd_id,
    'size_'::text || g.population_class::text AS fd_size,
    f.res_corelogic,
    f.res_other,
    f.bld_units,
    f.hr_floors,
    f.eff_yr,
    f.risk_class,
    NULL::double precision AS fires,
    NULL::double precision AS size_1,
    NULL::double precision AS size_2,
    NULL::double precision AS size_3,
    NULL::double precision AS deaths,
    NULL::double precision AS injuries,
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
     LEFT JOIN nist.tract_years t ON f.geoid = t.tr10_fid
     LEFT JOIN firestation_firedepartment g ON t.state::text = g.state::text AND t.fdid::text = g.fdid::text
     LEFT JOIN nist.svi2010 svi ON f.geoid = ('14000US'::text || lpad(svi.fips::text, 11, '0'))
     LEFT JOIN nist.acs_est_new acs ON f.geoid = acs.geoid AND acs.year = 2015::double precision
     LEFT JOIN nist.sins sm ON t.state::text = sm.postal_code AND sm.year = 2010
     LEFT JOIN nist.sins_county sc ON "substring"(f.geoid, 8, 5) = sc.fips
  WHERE t.year = 2014
WITH DATA;

ALTER TABLE nist.hr_pred
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.hr_pred TO sgilbert;
GRANT SELECT ON TABLE nist.hr_pred TO firecares;
COMMENT ON MATERIALIZED VIEW nist.hr_pred
  IS 'This collects all the data needed to predict the number of fires etc. for high
risk fires. There is (or should be) one entry per high-risk parcel. 

The main issue with this query is the use of hard-coded dates in a couple of
places in the JOIN clauses. As currently written, they will have to be updated 
periodically to keep the query up to date.';
