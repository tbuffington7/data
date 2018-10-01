-- Materialized View: nist.ems_table_500

CREATE MATERIALIZED VIEW nist.ems_table_500 AS 
WITH f AS (
  SELECT 
    extract(year from inc_date) as year,
    tr10_fid,
    count(*) AS calls
  FROM nist.coded_ems
  WHERE extract(year from inc_date) > 2006 
  GROUP BY extract(year from inc_date), tr10_fid
), d AS (
  SELECT i.year,
    d_1.id AS fd_id,
    'size_'::text || d_1.population_class AS fd_size,
    sum(i.incidents) AS incidents,
    sum(i.incidents_loc) AS located,
    sum(i.calls) AS calls,
    sum(i.calls_loc) AS calls_loc
  FROM nist.dept_incidents2 i
       JOIN firestation_firedepartment d_1 USING (state, fdid)
  WHERE i.year > 2006
  GROUP BY 
    i.year, 
	d_1.id, 
	d_1.population_class
), h AS (
  SELECT 
    '14000US'::text || "substring"(cities_500."UniqueID", 9, 11) AS geoid,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'ARTHRITIS'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS arthritis,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'BPHIGH'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS bphigh,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'CANCER'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS cancer,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'CASTHMA'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS casthma,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'CHD'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS chd,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'COPD'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS copd,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'DIABETES'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS diabetes,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'HIGHCHOL'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS highchol,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'KIDNEY'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS kidney,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'MHLTH'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS mhlth,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'PHLTH'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS phlth,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'STROKE'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS stroke,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'TEETHLOST'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS teethlost,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'ACCESS2'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS access2,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'BPMED'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS bpmed,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'CHECKUP'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS checkup,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'CHOLSCREEN'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS cholscreen,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'COLON_SCREEN'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS colon_screen,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'COREM'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS corem,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'COREW'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS corew,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'DENTAL'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS dental,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'MAMMOUSE'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS mammouse,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'PAPTEST'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS paptest,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'BINGE'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS binge,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'CSMOKING'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS csmoking,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'LPA'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS lpa,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'OBESITY'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS obesity,
    sum(
    CASE
      WHEN cities_500."MeasureId" = 'SLEEP'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text THEN cities_500."Data_Value"
      ELSE 0::double precision
    END) AS sleep
           FROM nist.cities_500
          WHERE cities_500."GeographicLevel" = 'Census Tract'::text AND cities_500."DataValueTypeID" = 'CrdPrv'::text
          GROUP BY ('14000US'::text || "substring"(cities_500."UniqueID", 9, 11))
)
SELECT tr.year,
  tr.tr10_fid AS geoid,
  tr.region,
  tr.state,
  tr.fdid,
  tr.fc_dept_id,
  d.fd_size,
  cc.clusters as cluster,
  d.calls AS dept_calls,
  CASE
    WHEN d.calls > 0::numeric THEN d.calls_loc::double precision / d.calls::double precision
    WHEN d.calls = 0::numeric AND d.calls_loc > 0::numeric THEN 'Infinity'::double precision
    ELSE 'NaN'::double precision
  END AS c_located,
  f.calls as ems,
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
  h.arthritis,
  h.bphigh,
  h.cancer,
  h.casthma,
  h.chd,
  h.copd,
  h.diabetes,
  h.highchol,
  h.kidney,
  h.mhlth,
  h.phlth,
  h.stroke,
  h.teethlost,
  h.access2,
  h.bpmed,
  h.checkup,
  h.cholscreen,
  h.colon_screen,
  h.corem,
  h.corew,
  h.dental,
  h.mammouse,
  h.paptest,
  h.binge,
  h.csmoking,
  h.lpa,
  h.obesity,
  h.sleep
FROM nist.tract_years_2 tr
  LEFT JOIN f ON tr.tr10_fid::text = f.tr10_fid AND tr.year::double precision = f.year::double precision
  LEFT JOIN d ON tr.fc_dept_id = d.fd_id AND tr.year::double precision = d.year::double precision
  LEFT JOIN nist.svi2010 svi ON tr.tr10_fid::text = ('14000US'::text || lpad(svi.fips::text, 11, '0'::text))
  LEFT JOIN nist.acs_est_new acs ON tr.tr10_fid::text = acs.geoid AND
    CASE
      WHEN tr.year < 2008 THEN 2008
      WHEN tr.year > (select max(year) from nist.acs_est_new) - 2 THEN (select max(year) from nist.acs_est_new) - 2
      ELSE tr.year
    END::double precision = (acs.year - 2::double precision)
  LEFT JOIN h ON tr.tr10_fid::text = h.geoid
  LEFT JOIN nist.county_clusters cc ON substring(tr.tr10_fid, 8, 5) = cc.geoid
WHERE tr.year > 2006
WITH DATA;

ALTER TABLE nist.ems_table_500
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.ems_table_500 TO sgilbert;
GRANT SELECT ON TABLE nist.ems_table_500 TO firecares;

COMMENT ON MATERIALIZED VIEW nist.ems_table_500
  IS 'This summarizes the information for ems calls by tract and year. 
It also includes data that will be used to estimate the model. Data is from 
NFIRS (indirectly through dept_incidents), the ACS, CoreLogic, the BRFSS, and 
several other sources.

There are two versions of this table. In this version, the BFRSS data is expressed
in the 500-Cities data set from the CDC. The 500-Cities data covers only the 500 largest 
cities in the county, but is at the census tract level.

The "h" temporary table collects the 500-Cities data in a form that is easily usable for 
this table. The tables sums over two years of data. I can do that because the two years 
are disjoint in the questions they return data for. Since I have only a single year of data
for the health data, the same results are attached to every year.

The following task(s) need to be dealt with before completing this. 
* Ownership needs to be handed over to firecares.';
