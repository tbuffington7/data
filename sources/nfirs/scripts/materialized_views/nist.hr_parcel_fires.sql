CREATE MATERIALIZED VIEW nist.hr_parcel_fires AS 
 WITH y AS (
         SELECT generate_series(2007, 2018) AS year
        ), p0 AS (
         SELECT y.year,
            h.parcel_id,
                CASE
                    WHEN char_length(h.census_tr::text) >= 6 THEN (('14000US'::text || h.state_code::text) || h.cnty_code::text) || "substring"(h.census_tr::text, 1, 6)
                    ELSE NULL::text
                END AS p_geoid,
                CASE
                    WHEN "left"(h.land_use::text, 1) = '1'::text THEN 'Yes'::text
                    ELSE 'No'::text
                END AS res_corelogic,
            h.res_other,
            h.bld_units,
            h.hr_floors,
            h.yr_blt,
            h.eff_yr_blt,
            h.risk_class
           FROM nist.consolidated_hr_parcels_swg p
             LEFT JOIN nist.hr_parcels_local h ON p.p_id = h.parcel_id
             CROSS JOIN y
        ), p1 AS (
         SELECT p.p_id AS parcel_id,
            f.geoid AS f_geoid,
            f.inc_type,
            f.year,
            f.struc,
            f.fire_sprd,
            f.ff_death,
            f.oth_death,
            f.ff_inj,
            f.oth_inj
           FROM nist.consolidated_hr_parcels_swg p,
            nist.coded_fires f
          WHERE p.geom && f.geom
        ), v1 AS (
         SELECT p1_1.parcel_id,
            p1_1.f_geoid,
            sum(
                CASE
                    WHEN p1_1.f_geoid IS NOT NULL AND p1_1.f_geoid <> ''::text THEN 1
                    ELSE 0
                END) AS n
           FROM p1 p1_1
          WHERE p1_1.f_geoid <> ''::text AND p1_1.f_geoid IS NOT NULL
          GROUP BY p1_1.parcel_id, p1_1.f_geoid
        ), v2a AS (
         SELECT v1.parcel_id,
            max(v1.n) AS n_max
           FROM v1
          GROUP BY v1.parcel_id
        ), v2 AS (
         SELECT v1.parcel_id,
            min(v1.f_geoid) AS f_geoid,
            v1.n
           FROM v2a
             JOIN v1 ON v2a.parcel_id = v1.parcel_id AND v2a.n_max = v1.n
          GROUP BY v1.parcel_id, v1.n
        ), v3 AS (
         SELECT hr.parcel_id,
            bg.geoid AS c_geoid
           FROM nist.hr_parcels_local hr,
            nist.census_tract_locs_swg bg
          WHERE (char_length(hr.census_tr::text) < 6 OR hr.census_tr IS NULL) AND st_contains(bg.geom, hr.wkb_geometry)
        )
 SELECT p0.year,
    p0.parcel_id,
        CASE
            WHEN p0.p_geoid IS NOT NULL THEN p0.p_geoid
            WHEN v3.c_geoid IS NOT NULL THEN v3.c_geoid
            WHEN v2.f_geoid IS NOT NULL THEN v2.f_geoid
            ELSE NULL::text
        END AS geoid,
        CASE
            WHEN p0.p_geoid IS NOT NULL THEN 'CoreLogic'::text
            WHEN v3.c_geoid IS NOT NULL THEN 'Census Location'::text
            WHEN v2.f_geoid IS NOT NULL THEN 'Fire Location'::text
            ELSE NULL::text
        END AS geoid_source,
    p0.res_corelogic,
    p0.res_other,
    p0.bld_units,
    p0.hr_floors,
        CASE
            WHEN p0.eff_yr_blt IS NOT NULL THEN p0.eff_yr_blt
            ELSE p0.yr_blt
        END AS eff_yr,
    p0.risk_class,
    p1.inc_type,
    p1.struc,
    p1.fire_sprd,
    p1.ff_death,
    p1.oth_death,
    p1.ff_inj,
    p1.oth_inj
   FROM p0
     LEFT JOIN p1 ON p0.parcel_id = p1.parcel_id AND p0.year::double precision = p1.year
     LEFT JOIN v2 ON p0.parcel_id = v2.parcel_id
     LEFT JOIN v3 ON p0.parcel_id = v3.parcel_id
  WHERE p0.year::double precision >= p0.yr_blt OR p0.yr_blt IS NULL
WITH DATA;

ALTER TABLE nist.hr_parcel_fires
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.hr_parcel_fires TO sgilbert;
GRANT SELECT ON TABLE nist.hr_parcel_fires TO firecares;
COMMENT ON MATERIALIZED VIEW nist.hr_parcel_fires
  IS 'Columns are defined as:
    year             integer,
    parcel_id        double precision,
    geoid            text,
    geoid_source     text,
    res_corelogic    text,
    res_other        text,
    bld_units        double precision,
    hr_floors        double precision,
    eff_yr           double precision,
    risk_class       text,
    inc_type         varchar(3),
    struc            text,
    fire_sprd        varchar(1),
    ff_death         integer,
    oth_death        integer,
    ff_inj           integer,
    oth_inj          integer

  This very complicated view does three things. First, it compiles a set of
  high-risk parcel-years for the study period. Second, it lists fires for each
  parcel-year. Third it connects each parcel-year to a census tract. It is the
  latter step that introduces the vast majority of the complexity.

  The end result is a list of high-risk fires, with empty records for any
  parcel-years where there were no fires.

  The query is structured as follows:
  Part 1: Generate the parcel-year sets. (subqueries y and p0)
  Part 2: Collect fire information (subquery p1)
  Part 3: Link to census tract. In particular, this generates two additional
          census tract links. Subqueries v1 and v2 together identify a tract
          based on fire locations. Subquery v3 identifies a tract based on the
          GIS location of the parcel.
  Final SELECT statement: Put all the elements together.

  Specific subqueries do the following:
  y:   Generate the years of the analysis.
  p0:  Generate the parcel-year sets from the list of high-risk parcels and the
       y subquery

  p1:  This query joins fires to high-risk parcels based on whether the fire
       falls within the bounding box for the parcel. The bounding box criterion
       means that there may be cases where too many fires are included or where
       the same fire is attached to more than one parcel. (The accuracy--or
       lack thereof--of locations for fires is also a potential issue here).

  v1:  For those fires (identified in p1 above) with an associated census tract,
       rank the census tracts by number of fires for each parcel. So if a parcel
       has ten fires, with three different census tracts among the associated
       fires, the tract with the most fires will be ranked 1, etc.
  v2a: Find the maximum number of fires associated with any tract...
  v2:  and find the "first" tract with that many fires.

  v3:  Any parcel without an identified tract (from subquery p0) find any census
       tract that contains the parcel. While this should give a census tract to
       nearly all the remaining parcels, it is still possible for a parcel to
       not get a tract here if the parcel crosses the border of a tract or falls
       outside of all of them.

  Main Query: Aside from compiling the fire information (while including a
       NULL record for any parcel-years with no fires) it selects a census tract
       to go with the fire / parcel. The selection is strictly priority based:
       if the original parcel record had a tract identified, it is used. If not,
       location (i.e., v3) is used. If it does not exist, majority vote among
       fires is used. If there are none, then the field is left NULL.';
