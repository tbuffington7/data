CREATE MATERIALIZED VIEW med_risk_parcel_info AS 
 SELECT p.state_code,
    p.cnty_code,
    "substring"(p.census_tr::text, 1, 6) AS tract,
    sum(
        CASE
            WHEN luse.risk_category = 'Medium'::text AND luse.residential = 'Yes'::text AND (p.risk_category::text <> 'high'::text OR p.risk_category IS NULL) THEN 1
            ELSE 0
        END) AS apts_n,
    sum(
        CASE
            WHEN luse.risk_category = 'Medium'::text AND luse.residential = 'Yes'::text AND (p.risk_category::text <> 'high'::text OR p.risk_category IS NULL) AND p.bld_units IS NULL THEN 1::double precision
            WHEN luse.risk_category = 'Medium'::text AND luse.residential = 'Yes'::text AND (p.risk_category::text <> 'high'::text OR p.risk_category IS NULL) AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS apts_u,
    sum(
        CASE
            WHEN luse.risk_category = 'Medium'::text AND luse.residential = 'No'::text AND (p.risk_category::text <> 'high'::text OR p.risk_category IS NULL) THEN 1
            ELSE 0
        END) AS mr_n,
    sum(
        CASE
            WHEN luse.risk_category = 'Medium'::text AND luse.residential = 'No'::text AND (p.risk_category::text <> 'high'::text OR p.risk_category IS NULL) AND p.bld_units IS NULL THEN 1::double precision
            WHEN luse.risk_category = 'Medium'::text AND luse.residential = 'No'::text AND (p.risk_category::text <> 'high'::text OR p.risk_category IS NULL) AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS mr_u,
    sum(
        CASE
            WHEN luse.risk_category = 'Low'::text AND luse.residential = 'Yes'::text THEN 1
            ELSE 0
        END) AS sfr_n,
    sum(
        CASE
            WHEN luse.risk_category = 'Low'::text AND luse.residential = 'Yes'::text AND p.bld_units IS NULL THEN 1::double precision
            WHEN luse.risk_category = 'Low'::text AND luse.residential = 'Yes'::text AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS sfr_u,
    sum(
        CASE
            WHEN p.land_use::text ~~ '2%'::text THEN 1
            ELSE 0
        END) AS com_n,
    sum(
        CASE
            WHEN p.land_use::text ~~ '2%'::text AND p.bld_units IS NULL THEN 1::double precision
            WHEN p.land_use::text ~~ '2%'::text AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS com_u,
    sum(
        CASE
            WHEN p.land_use::text ~~ '3%'::text THEN 1
            ELSE 0
        END) AS ind_n,
    sum(
        CASE
            WHEN p.land_use::text ~~ '3%'::text AND p.bld_units IS NULL THEN 1::double precision
            WHEN p.land_use::text ~~ '3%'::text AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS ind_u,
    sum(
        CASE
            WHEN p.land_use::text ~~ '4%'::text THEN 1
            ELSE 0
        END) AS vacant_n,
    sum(
        CASE
            WHEN p.land_use::text ~~ '4%'::text AND p.bld_units IS NULL THEN 1::double precision
            WHEN p.land_use::text ~~ '4%'::text AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS vacant_u,
    sum(
        CASE
            WHEN p.land_use::text ~~ '5%'::text THEN 1
            ELSE 0
        END) AS agr_n,
    sum(
        CASE
            WHEN p.land_use::text ~~ '5%'::text AND p.bld_units IS NULL THEN 1::double precision
            WHEN p.land_use::text ~~ '5%'::text AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS agr_u,
    sum(
        CASE
            WHEN p.land_use::text ~~ '6%'::text THEN 1
            ELSE 0
        END) AS gov_n,
    sum(
        CASE
            WHEN p.land_use::text ~~ '6%'::text AND p.bld_units IS NULL THEN 1::double precision
            WHEN p.land_use::text ~~ '6%'::text AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS gov_u,
    sum(
        CASE
            WHEN p.land_use::text ~~ '7%'::text THEN 1
            ELSE 0
        END) AS rec_n,
    sum(
        CASE
            WHEN p.land_use::text ~~ '7%'::text AND p.bld_units IS NULL THEN 1::double precision
            WHEN p.land_use::text ~~ '7%'::text AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS rec_u,
    sum(
        CASE
            WHEN p.land_use::text ~~ '8%'::text THEN 1
            ELSE 0
        END) AS trans_n,
    sum(
        CASE
            WHEN p.land_use::text ~~ '8%'::text AND p.bld_units IS NULL THEN 1::double precision
            WHEN p.land_use::text ~~ '8%'::text AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS trans_u,
    sum(
        CASE
            WHEN p.land_use::text ~~ '9%'::text THEN 1
            ELSE 0
        END) AS oth_n,
    sum(
        CASE
            WHEN p.land_use::text ~~ '9%'::text AND p.bld_units IS NULL THEN 1::double precision
            WHEN p.land_use::text ~~ '9%'::text AND p.bld_units IS NOT NULL THEN p.bld_units
            ELSE 0::double precision
        END) AS oth_u
   FROM parcels p
     LEFT JOIN "LUSE_swg" luse ON p.land_use::text = luse."Code"
  GROUP BY p.state_code, p.cnty_code, "substring"(p.census_tr::text, 1, 6)
WITH DATA;

ALTER TABLE med_risk_parcel_info
  OWNER TO sgilbert;
