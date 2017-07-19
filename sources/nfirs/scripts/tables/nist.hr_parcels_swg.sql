CREATE FOREIGN TABLE nist.hr_parcels_swg
   (ogc_fid integer ,
    wkb_geometry geometry ,
    parcel_id double precision ,
    state_code character varying ,
    cnty_code character varying ,
    apn character varying ,
    apn2 character varying ,
    addr character varying ,
    city character varying ,
    state character varying ,
    zip character varying ,
    plus character varying ,
    std_addr character varying ,
    std_city character varying ,
    std_state character varying ,
    std_zip character varying ,
    std_plus character varying ,
    fips_code character varying ,
    unfrm_apn character varying ,
    apn_seq_no double precision ,
    frm_apn character varying ,
    orig_apn character varying ,
    acct_no character varying ,
    census_tr character varying ,
    block_nbr character varying ,
    lot_nbr character varying ,
    land_use character varying ,
    m_home_ind character varying ,
    prop_ind character varying ,
    own_cp_ind character varying ,
    tot_val double precision ,
    lan_val double precision ,
    imp_val double precision ,
    tot_val_cd character varying ,
    lan_val_cd character varying ,
    assd_val double precision ,
    assd_lan double precision ,
    assd_imp double precision ,
    mkt_val double precision ,
    mkt_lan double precision ,
    mkt_imp double precision ,
    appr_val double precision ,
    appr_lan double precision ,
    appr_imp double precision ,
    tax_amt double precision ,
    tax_yr double precision ,
    assd_yr double precision ,
    ubld_sq_ft double precision ,
    bld_sq_ft double precision ,
    liv_sq_ft double precision ,
    gr_sq_ft double precision ,
    yr_blt double precision ,
    eff_yr_blt double precision ,
    bedrooms double precision ,
    rooms double precision ,
    bld_code character varying ,
    bld_imp_cd character varying ,
    condition character varying ,
    constr_typ character varying ,
    ext_walls character varying ,
    quality character varying ,
    story_nbr double precision ,
    bld_units double precision ,
    units_nbr double precision ,
    risk_category character varying(6) ,
    risk_data json ,
    risk_class text ,
    hr_floors double precision ,
    unit_type text ,
    res_other text )
   SERVER parcels
   OPTIONS (schema_name 'public');
ALTER FOREIGN TABLE nist.hr_parcels_swg
  OWNER TO sgilbert;

CREATE nist.hr_parcels_local AS 
  SELECT * FROM nist.hr_parcels_swg;

ALTER TABLE nist.hr_parcels_local
  OWNER TO sgilbert;
GRANT ALL ON TABLE nist.hr_parcels_local TO sgilbert;
GRANT SELECT ON TABLE nist.hr_parcels_local TO firecares;

-- Index: nist.hr_parcels_ndx

CREATE INDEX hr_parcels_ndx
  ON nist.hr_parcels_local
  USING gist
  (wkb_geometry);
