CREATE MATERIALIZED VIEW nist.med_risk_fires AS 
 WITH f AS (
         SELECT cf.year,
            cf.geoid,
            count(*) AS tot_fires,
            sum(
                CASE
                    WHEN cf.struc = 'Y'::text AND cf.risk = 'Med Risk'::text AND cf.geoid IS NOT NULL THEN 1
                    ELSE 0
                END) AS med_risk,
            sum(
                CASE
                    WHEN cf.struc = 'Y'::text AND cf.risk = 'Med Risk'::text AND cf.geoid IS NOT NULL AND cf.fire_sprd IS NOT NULL THEN 1
                    ELSE 0
                END) AS mr_1,
            sum(
                CASE
                    WHEN cf.struc = 'Y'::text AND cf.risk = 'Med Risk'::text AND cf.geoid IS NOT NULL AND (cf.fire_sprd::text = ANY (ARRAY['3'::text, '4'::text, '5'::text])) THEN 1
                    ELSE 0
                END) AS mr_2,
            sum(
                CASE
                    WHEN cf.struc = 'Y'::text AND cf.risk = 'Med Risk'::text AND cf.geoid IS NOT NULL AND cf.fire_sprd::text = '5'::text THEN 1
                    ELSE 0
                END) AS mr_3,
            sum(
                CASE
                    WHEN cf.struc = 'Y'::text AND cf.risk = 'Med Risk'::text AND cf.geoid IS NOT NULL THEN cf.ff_inj + cf.oth_inj
                    ELSE 0
                END) AS injuries,
            sum(
                CASE
                    WHEN cf.struc = 'Y'::text AND cf.risk = 'Med Risk'::text AND cf.geoid IS NOT NULL THEN cf.ff_death + cf.oth_death
                    ELSE 0
                END) AS deaths
           FROM nist.coded_fires cf
          WHERE cf.year > 2006::double precision AND cf.version = 5.0 AND NOT (cf.inc_type::text = '112'::text AND cf.year > 2007::double precision)
          GROUP BY cf.year, cf.geoid
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
        ), c AS (
         SELECT cf.geoid,
            cf.year,
            sum(
                CASE
                    WHEN cf.sev::text <> '5'::text AND (cf.type = 'ff'::text OR cf.aid_flag = 'N'::text) THEN 1
                    ELSE 0
                END) AS injuries,
            sum(
                CASE
                    WHEN cf.sev::text = '5'::text AND (cf.type = 'ff'::text OR cf.aid_flag = 'N'::text) THEN 1
                    ELSE 0
                END) AS deaths
           FROM nist.casualties_fire cf
          WHERE cf.risk = 'Med Risk'::text
          GROUP BY cf.geoid, cf.year
        )
 SELECT tr.year,
    tr.tr10_fid AS geoid,
    tr.region,
    tr.state,
    tr.fc_dept_id AS fd_id,
    d.fd_size,
    d.incidents AS dept_incidents,
    d.dept_fires,
        CASE
            WHEN d.incidents > 0::numeric THEN d.located::double precision / d.incidents::double precision
            WHEN d.incidents = 0::numeric AND d.located > 0::numeric THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS f_located,
    f.med_risk,
    f.mr_1,
    f.mr_2,
    f.mr_3,
    c.injuries,
    c.deaths,
        CASE
            WHEN acs."B25002_002E" > 0 THEN acs."B01001_001E"::double precision / acs."B25002_002E"::double precision
            WHEN acs."B25002_002E" = 0 AND acs."B01001_001E" > 0 THEN 'Infinity'::double precision
            ELSE 'NaN'::double precision
        END AS ave_hh_sz,
    acs."B01001_001E" AS pop,
    acs."B02001_003E" AS black,
    acs."B02001_004E" AS amer_es,
    acs."B02001_005E" + acs."B02001_006E" + acs."B02001_007E" + acs."B02001_008E" AS other,
    acs."B03003_003E" AS hispanic,
    acs."B01001_002E" AS males,
    acs."B01001_003E" + acs."B01001_027E" AS age_under5,
    acs."B01001_004E" + acs."B01001_028E" AS age_5_9,
    acs."B01001_005E" + acs."B01001_029E" AS age_10_14,
    acs."B01001_006E" + acs."B01001_007E" + acs."B01001_030E" + acs."B01001_031E" AS age_15_19,
    acs."B01001_008E" + acs."B01001_009E" + acs."B01001_010E" + acs."B01001_032E" + acs."B01001_033E" + acs."B01001_034E" AS age_20_24,
    acs."B01001_011E" + acs."B01001_012E" + acs."B01001_035E" + acs."B01001_036E" AS age_25_34,
    acs."B01001_013E" + acs."B01001_014E" + acs."B01001_037E" + acs."B01001_038E" AS age_35_44,
    acs."B01001_015E" + acs."B01001_016E" + acs."B01001_039E" + acs."B01001_040E" AS age_45_54,
    acs."B01001_017E" + acs."B01001_018E" + acs."B01001_019E" + acs."B01001_041E" + acs."B01001_042E" + acs."B01001_043E" AS age_55_64,
    acs."B01001_020E" + acs."B01001_021E" + acs."B01001_022E" + acs."B01001_044E" + acs."B01001_045E" + acs."B01001_046E" AS age_65_74,
    acs."B01001_023E" + acs."B01001_024E" + acs."B01001_047E" + acs."B01001_048E" AS age_75_84,
    acs."B01001_025E" + acs."B01001_049E" AS age_85_up,
    acs."B25002_001E" AS hse_units,
    acs."B25002_003E" AS vacant,
    acs."B25014_008E" AS renter_occ,
    acs."B25014_005E" + acs."B25014_006E" + acs."B25014_007E" + acs."B25014_011E" + acs."B25014_012E" + acs."B25014_013E" AS crowded,
    acs."B25024_002E" + acs."B25024_003E" + acs."B25024_004E" AS sfr,
    acs."B25024_007E" + acs."B25024_008E" + acs."B25024_009E" AS units_10,
    acs."B25024_010E" AS mh,
    acs."B25034_006E" + acs."B25034_007E" + acs."B25034_008E" + acs."B25034_009E" + acs."B25034_010E" AS older,
    pcl.apts_n AS apt_parcels,
    pcl.mr_n AS mr_parcels,
    acs."B19013_001E" AS inc_hh,
    svi.r_pl_themes AS svi,
    acs."B12001_001" - (acs."B12001_003" + acs."B12001_012") AS married,
    acs."B23025_005" AS unemployed,
    acs."B12001_007" AS nilf,
    sm.adult_smoke AS smoke_st,
    sc.smoking_pct AS smoke_cty
   FROM nist.tract_years tr
     LEFT JOIN f ON tr.tr10_fid = f.geoid AND tr.year::double precision = f.year
     LEFT JOIN d ON tr.fc_dept_id = d.fd_id AND tr.year::double precision = d.year
     LEFT JOIN nist.svi2010 svi ON tr.tr10_fid = ('14000US'::text || svi.fips::text)
     LEFT JOIN nist.acs_est_new acs ON tr.tr10_fid = acs.geoid AND
        CASE
            WHEN tr.year < 2008 THEN 2008
            WHEN tr.year > 2013 THEN 2013
            ELSE tr.year
        END::double precision = (acs.year - 2::double precision)
     LEFT JOIN nist.sins sm ON tr.state::text = sm.postal_code AND sm.year = 2010
     LEFT JOIN nist.sins_county sc ON "substring"(tr.tr10_fid, 8, 5) = sc.fips
     LEFT JOIN c ON tr.tr10_fid = c.geoid AND tr.year::double precision = c.year
     LEFT JOIN nist.med_risk_parcel_info pcl ON tr.tr10_fid = ((('14000US'::text || pcl.state_code) || pcl.cnty_code) || pcl.tract)
  WHERE tr.year > 2006
WITH DATA;

ALTER TABLE nist.med_risk_fires
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.med_risk_fires TO sgilbert;
GRANT SELECT ON TABLE nist.med_risk_fires TO firecares;
COMMENT ON MATERIALIZED VIEW nist.med_risk_fires
  IS 'This summarizes the fire information for medium risk fires by tract and year. 
It also includes data that will be used to estimate the model. Data is from 
NFIRS (indirectly through dept_incidents), the ACS, CoreLogic, and several other 
sources.

Note that I am experimenting with specifications that almost completely
eliminate the hard-coded dates that have been included up to this point.';
