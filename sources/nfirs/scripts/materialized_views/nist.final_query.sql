CREATE MATERIALIZED VIEW nist.final_query AS
WITH f AS (
SELECT cf.year, cf.geoid, count(*) AS tot_fires,
	sum(CASE WHEN cf.prop_use LIKE '4%' AND cf.geoid IS NOT NULL THEN 1 ELSE 0 END ) AS res_all,
	sum(CASE WHEN cf.struc = 'Y' AND risk='Low Risk' AND cf.geoid IS NOT NULL THEN 1 ELSE 0END) AS low_risk,
	sum(CASE WHEN cf.struc = 'Y'::text AND cf.risk = 'Low Risk'::text AND cf.geoid IS NOT NULL
	            AND cf.fire_sprd IS NOT NULL THEN 1 ELSE 0 END) AS res_1,
	sum(CASE WHEN cf.struc = 'Y' AND risk='Low Risk' AND cf.geoid IS NOT NULL AND cf.fire_sprd IN ( '3', '4', '5') THEN 1 ELSE 0 END) AS res_2,
	sum(CASE WHEN cf.struc = 'Y' AND risk='Low Risk' AND cf.geoid IS NOT NULL AND cf.fire_sprd = '5' THEN 1 ELSE 0 END) AS res_3,
	sum(CASE WHEN cf.struc = 'Y'::text AND cf.risk = 'Low Risk'::text AND cf.geoid IS NOT NULL THEN cf.ff_inj + cf.oth_inj ELSE 0 END) AS injuries,
	sum(CASE WHEN cf.struc = 'Y'::text AND cf.risk = 'Low Risk'::text AND cf.geoid IS NOT NULL THEN cf.ff_death + cf.oth_death ELSE 0 END) AS deaths
FROM nist.coded_fires cf
WHERE cf.year > 2006 AND cf.year < 2014 AND cf.version = 5.0 AND NOT ( cf.inc_type = '112' AND cf.year > 2007 )
GROUP BY cf.year, cf.geoid
), d AS (
SELECT i.year, d.id AS fd_id, d.population_class as fd_size,
	sum(i.incidents) AS incidents, sum(i.incidents_loc) AS located,
	sum(i.fires) AS dept_fires, sum(i.lr_fires) AS dept_lr
FROM nist.dept_incidents i JOIN firestation_firedepartment d USING (state, fdid)
WHERE i.year > 2006 and i.year < 2014
GROUP BY i.year, d.id, d.population_class
) , c AS (
SELECT casualties_fire.geoid, nist.casualties_fire.year,
	sum(CASE WHEN nist.casualties_fire.sev <> '5'::text AND (nist.casualties_fire.type = 'ff'::text OR nist.casualties_fire.aid_flag = 'N'::text ) THEN 1 ELSE 0 END) AS injuries,
	sum(CASE WHEN nist.casualties_fire.sev = '5'::text AND ( nist.casualties_fire.type = 'ff'::text OR nist.casualties_fire.aid_flag = 'N'::text ) THEN 1 ELSE 0 END) AS deaths
FROM nist.casualties_fire
GROUP BY nist.casualties_fire.geoid, nist.casualties_fire.year
)
SELECT tr.year, tr.tr10_fid, tr.region, tr.state, tr.fc_dept_id AS fd_id,
	d.fd_size, d.incidents AS dept_incidents, d.dept_fires, d.dept_lr,
	CASE
		WHEN d.incidents > 0 THEN d.located::double precision / d.incidents::double precision
		WHEN d.incidents = 0 AND d.located > 0 THEN 'Infinity'::double precision
		ELSE 'NaN'::double precision
	END AS f_located,
	f.res_all, f.low_risk, f.res_1, f.res_2, f.res_3, c.injuries, c.deaths,
	CASE
		WHEN acs."B25002_002E" > 0 THEN acs."B01001_001E"::double precision / acs."B25002_002E"::double precision
		WHEN acs."B25002_002E" = 0 AND acs."B01001_001E" > 0 THEN 'Infinity'::double precision
		ELSE 'NaN'::double precision
	END AS ave_hh_sz,
	acs."B01001_001E" AS pop, acs."B02001_003E" AS black,
	acs."B02001_004E" AS amer_es,
	acs."B02001_005E" + acs."B02001_006E" + acs."B02001_007E" + acs."B02001_008E" AS other,
	acs."B03003_003E" AS hispanic, acs."B01001_002E" AS males,
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
	acs."B25002_001E" AS hse_units, acs."B25002_003E" AS vacant,
	acs."B25014_008E" AS renter_occ,
	acs."B25014_005E" + acs."B25014_006E" + acs."B25014_007E" + acs."B25014_011E" + acs."B25014_012E" + acs."B25014_013E" AS crowded,
	acs."B25024_002E" + acs."B25024_003E" + acs."B25024_004E" AS sfr,
	acs."B25024_007E" + acs."B25024_008E" + acs."B25024_009E" AS units_10,
	acs."B25024_010E" AS mh,
	acs."B25034_006E" + acs."B25034_007E" + acs."B25034_008E" + acs."B25034_009E" + acs."B25034_010E" AS older,
	acs."B19013_001E" AS inc_hh,
	svi.r_pl_themes AS svi,
	acs."B12001_001" - (acs."B12001_003" + acs."B12001_012") AS married,
	acs."B23025_005" AS unemployed, acs."B12001_007" AS nilf,
	sm.adult_smoke AS smoke_st, sc.smoking_pct AS smoke_cty,
	acs."B25040_002E" AS fuel_gas,
	acs."B25040_003E" AS fuel_tank, acs."B25040_005E" AS fuel_oil,
	acs."B25040_006E" AS fuel_coal, acs."B25040_007E" AS fuel_wood,
	acs."B25040_008E" AS fuel_solar, acs."B25040_009E" AS fuel_other,
	acs."B25040_010E" AS fuel_none
FROM nist.tract_years tr LEFT JOIN f ON tr.tr10_fid = f.geoid AND tr.year = f.year
							LEFT JOIN d ON tr.fc_dept_id = d.fd_id AND tr.year = d.year
							LEFT JOIN nist.svi2010 svi ON tr.tr10_fid = '14000US' || lpad(fips::varchar, 11, '0')
							LEFT JOIN nist.acs_est acs ON tr.tr10_fid = acs.geoid AND
										CASE
											WHEN tr.year < 2008 THEN 2008
											WHEN tr.year > 2012 THEN 2012
											ELSE tr.year
										END::double precision = (acs.year - 2::double precision)
							LEFT JOIN nist.sins sm ON tr.state = sm.postal_code AND sm.year = 2010
							LEFT JOIN nist.sins_county sc ON substring(tr.tr10_fid, 8, 5) = sc.fips
							LEFT JOIN c ON tr.tr10_fid::text = c.geoid AND tr.year = c.year
WHERE tr.year > 2006 AND tr.year < 2014;




