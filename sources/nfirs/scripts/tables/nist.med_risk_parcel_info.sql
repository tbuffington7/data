CREATE FOREIGN TABLE nist.med_risk_parcel_info
   (state_code text ,
    cnty_code text ,
    tract text ,
    apts_n integer ,
    apts_u integer ,
    mr_n integer ,
    mr_u integer ,
    sfr_n integer ,
    sfr_u integer ,
    com_n integer ,
    com_u integer ,
    ind_n integer ,
    ind_u integer ,
    vacant_n integer ,
    vacant_u integer ,
    agr_n integer ,
    agr_u integer ,
    gov_n integer ,
    gov_u integer ,
    rec_n integer ,
    rec_u integer ,
    trans_n integer ,
    trans_u integer ,
    oth_n integer ,
    oth_u integer )
   SERVER parcels
   OPTIONS (schema_name 'public');
ALTER FOREIGN TABLE nist.med_risk_parcel_info
  OWNER TO sgilbert;
