CREATE MATERIALIZED VIEW hr_parcels_swg AS 
 WITH t AS (
         SELECT parcels.ogc_fid,
            parcels.wkb_geometry,
            parcels.parcel_id,
            parcels.state_code,
            parcels.cnty_code,
            parcels.apn,
            parcels.apn2,
            parcels.addr,
            parcels.city,
            parcels.state,
            parcels.zip,
            parcels.plus,
            parcels.std_addr,
            parcels.std_city,
            parcels.std_state,
            parcels.std_zip,
            parcels.std_plus,
            parcels.fips_code,
            parcels.unfrm_apn,
            parcels.apn_seq_no,
            parcels.frm_apn,
            parcels.orig_apn,
            parcels.acct_no,
            parcels.census_tr,
            parcels.block_nbr,
            parcels.lot_nbr,
            parcels.land_use,
            parcels.m_home_ind,
            parcels.prop_ind,
            parcels.own_cp_ind,
            parcels.tot_val,
            parcels.lan_val,
            parcels.imp_val,
            parcels.tot_val_cd,
            parcels.lan_val_cd,
            parcels.assd_val,
            parcels.assd_lan,
            parcels.assd_imp,
            parcels.mkt_val,
            parcels.mkt_lan,
            parcels.mkt_imp,
            parcels.appr_val,
            parcels.appr_lan,
            parcels.appr_imp,
            parcels.tax_amt,
            parcels.tax_yr,
            parcels.assd_yr,
            parcels.ubld_sq_ft,
            parcels.bld_sq_ft,
            parcels.liv_sq_ft,
            parcels.gr_sq_ft,
            parcels.yr_blt,
            parcels.eff_yr_blt,
            parcels.bedrooms,
            parcels.rooms,
            parcels.bld_code,
            parcels.bld_imp_cd,
            parcels.condition,
            parcels.constr_typ,
            parcels.ext_walls,
            parcels.quality,
            parcels.story_nbr,
            parcels.bld_units,
            parcels.units_nbr,
            parcels.risk_category,
            parcels.risk_data
           FROM parcels
          WHERE parcels.risk_category IS NOT NULL
        )
 SELECT t.ogc_fid,
    t.wkb_geometry,
    t.parcel_id,
    t.state_code,
    t.cnty_code,
    t.apn,
    t.apn2,
    t.addr,
    t.city,
    t.state,
    t.zip,
    t.plus,
    t.std_addr,
    t.std_city,
    t.std_state,
    t.std_zip,
    t.std_plus,
    t.fips_code,
    t.unfrm_apn,
    t.apn_seq_no,
    t.frm_apn,
    t.orig_apn,
    t.acct_no,
    t.census_tr,
    t.block_nbr,
    t.lot_nbr,
    t.land_use,
    t.m_home_ind,
    t.prop_ind,
    t.own_cp_ind,
    t.tot_val,
    t.lan_val,
    t.imp_val,
    t.tot_val_cd,
    t.lan_val_cd,
    t.assd_val,
    t.assd_lan,
    t.assd_imp,
    t.mkt_val,
    t.mkt_lan,
    t.mkt_imp,
    t.appr_val,
    t.appr_lan,
    t.appr_imp,
    t.tax_amt,
    t.tax_yr,
    t.assd_yr,
    t.ubld_sq_ft,
    t.bld_sq_ft,
    t.liv_sq_ft,
    t.gr_sq_ft,
    t.yr_blt,
    t.eff_yr_blt,
    t.bedrooms,
    t.rooms,
    t.bld_code,
    t.bld_imp_cd,
    t.condition,
    t.constr_typ,
    t.ext_walls,
    t.quality,
    t.story_nbr,
    t.bld_units,
    t.units_nbr,
    t.risk_category,
    t.risk_data::json AS risk_data,
        CASE
            WHEN t.risk_data ? 'ebn'::text THEN 'High Rise'::text
            WHEN (t.risk_data ->> 'fcode'::text) ~~ '720%'::text THEN 'Assembly'::text
            WHEN (t.risk_data ->> 'fcode'::text) ~~ '820%'::text THEN 'Assembly'::text
            WHEN (t.risk_data ->> 'fcode'::text) ~~ '730%'::text THEN 'School'::text
            WHEN (t.risk_data ->> 'fcode'::text) = '80000'::text THEN 'Hospital'::text
            WHEN (t.risk_data ->> 'fcode'::text) = '80012'::text THEN 'Hospital'::text
            WHEN (t.risk_data ->> 'fcode'::text) = '74036'::text THEN 'Institutional'::text
            WHEN (t.risk_data ->> 'fcode'::text) = '79008'::text THEN 'Institutional'::text
            WHEN (t.risk_data ->> 'fcode'::text) = '80027'::text THEN 'Institutional'::text
            ELSE 'Industrial'::text
        END AS risk_class,
        CASE
            WHEN t.risk_data ? 'floors_overground'::text THEN (t.risk_data ->> 'floors_overground'::text)::double precision
            ELSE 0::double precision
        END AS hr_floors,
        CASE
            WHEN t.risk_data ? 'ebn'::text THEN t.risk_data ->> 'main_usage'::text
            WHEN t.risk_data ? 'capa_mmcfd'::text THEN 'Gas Plant'::text
            WHEN t.risk_data ? 'caref_mbpd'::text THEN 'Refinery'::text
            WHEN t.risk_data ? 'capacity'::text THEN 'Ethanol Plant'::text
            WHEN NOT t.risk_data ? 'fcode'::text THEN 'Biodiesel Plant'::text
            ELSE t.risk_data ->> 'fcode_name'::text
        END AS unit_type,
        CASE
            WHEN lower(t.risk_data ->> 'main_usage'::text) ~~ '%apartment%'::text OR lower(t.risk_data ->> 'main_usage'::text) ~~ '%residen%'::text OR lower(t.risk_data ->> 'main_usage'::text) ~~ '%housing%'::text OR lower(t.risk_data ->> 'main_usage'::text) ~~ '%living%'::text OR lower(t.risk_data ->> 'main_usage'::text) ~~ '%dormitory%'::text OR lower(t.risk_data ->> 'fcode_name'::text) ~~ '%dorm%'::text THEN 'Yes'::text
            ELSE 'No'::text
        END AS res_other
   FROM t
WITH DATA;

ALTER TABLE hr_parcels_swg
  OWNER TO sgilbert;
COMMENT ON MATERIALIZED VIEW hr_parcels_swg
  IS 'The main purpose of this is to reduce the number of records that later queries
need to search so as to reduce the time (and complexity) needed to complete
them. It also adds a small number of fields that will be used later.';
