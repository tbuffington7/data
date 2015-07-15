#!/bin/sh

# Assumes that the assocated shapefiles are alongside the NFIRS data

DB=nfirs_dev
NFIRS_ROOT=/Users/joe/Downloads/NFIRS

cd $NFIRS_ROOT

psql $DB << EOF
create table fireincident_2013 as table fireincident with no data;
create table hazchem_2013 as table hazchem with no data;
create table hazmat_2013 as table hazmat with no data;
create table hazmatequipinvolved_2013 as table hazmatequipinvolved with no data;
create table hazmobprop_2013 as table hazmobprop with no data;
create table incidentaddress_2013 as table incidentaddress with no data;
create table wildlands_2013 as table wildlands with no data;
create table arson_2013 as table arson with no data;
create table arsonagencyreferal_2013 as table arsonagencyreferal with no data;
create table arsonjuvsub_2013 as table arsonjuvsub with no data;
create table basicaid_2013 as table basicaid with no data;
create table basicincident_2013 as table basicincident with no data;
create table civiliancasualty_2013 as table civiliancasualty with no data;
create table codelookup_2013 as table codelookup with no data;
create table ems_2013 as table ems with no data;
create table fdheader_2013 as table fdheader with no data;
create table ffcasualty_2013 as table ffcasualty with no data;
create table ffequipfail_2013 as table ffequipfail with no data;
EOF


# convert from MMDDYYYY to something that Postgres can handle MM/DD/YYYY
sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' fireincident.txt
sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' incidentaddress.txt
sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' arsonjuvsub.txt
sed -i.bak 's/\"\([U123]\) -- [^"]*\"/"\1"/' arsonjuvsub.txt
sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' basicincident.txt
# convert from MMDDYYYYHHmm to something that Postgres can handle (MM/DD/YYYY HH:mm), matching the closing ^ causes adjacent fields to NOT be matched
sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)/^"\1\/\2\/\3 \4:\5"/g' basicincident.txt
sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' civiliancasualty.txt
# found one \x00 byte, vim => :%s/[\x00]//g OR the tr below
tr < incidentaddress.txt -d '\000' > incidentaddress.txt
# fdheader riddled w/ \x00 bytes, vim => :%s/[\x00]//g OR the tr below
tr < fdheader.txt -d '\000' > fdheader.txt

# looks like the dumps are ASCII, but have some utf-8 encoded characters, force LATIN1 to load w/o having automagic UTF-8 decoding issues
psql $DB << EOF
set client_encoding = 'LATIN1';
copy fireincident_2013 from '$NFIRS_ROOT/fireincident.txt' CSV DELIMITER '^' HEADER;
copy hazchem_2013 from '$NFIRS_ROOT/hazchem.txt' CSV DELIMITER '^' HEADER;
copy hazmat_2013 from '$NFIRS_ROOT/hazmat.txt' CSV DELIMITER '^' HEADER;
copy hazmatequipinvolved_2013 from '$NFIRS_ROOT/hazmatequipinvolved.txt' CSV DELIMITER '^' HEADER;
copy hazmobprop_2013 from '$NFIRS_ROOT/hazmobprop.txt' CSV DELIMITER '^' HEADER;
copy incidentaddress_2013 (state, fdid, inc_date, inc_no, exp_no, loc_type, num_mile, street_pre, streetname, streettype, streetsuf, apt_no, city, state_id, zip5, zip4, x_street) from '$NFIRS_ROOT/incidentaddress.txt' CSV DELIMITER '^' HEADER;
copy wildlands_2013 from '$NFIRS_ROOT/wildlands.txt' CSV DELIMITER '^' HEADER;
copy arson_2013 from '$NFIRS_ROOT/arson.txt' CSV DELIMITER '^' HEADER;
copy arsonagencyreferal_2013 from '$NFIRS_ROOT/arsonagencyreferal.txt' CSV DELIMITER '^' HEADER;
copy arsonjuvsub_2013 from '$NFIRS_ROOT/arsonjuvsub.txt' CSV DELIMITER '^' HEADER;
copy basicaid_2013 from '$NFIRS_ROOT/basicaid.txt' CSV DELIMITER '^' HEADER;
copy basicincident_2013 from '$NFIRS_ROOT/basicincident.txt' CSV DELIMITER '^' HEADER;
copy civiliancasualty_2013 from '$NFIRS_ROOT/civiliancasualty.txt' CSV DELIMITER '^' HEADER;
copy codelookup_2013 from '$NFIRS_ROOT/codelookup.txt' CSV DELIMITER '^' HEADER;
copy ems_2013 from '$NFIRS_ROOT/ems.txt' CSV DELIMITER '^' HEADER;
copy fdheader_2013 from '$NFIRS_ROOT/fdheader.txt' CSV DELIMITER '^' HEADER;
copy ffcasualty_2013 from '$NFIRS_ROOT/ffcasualty.txt' CSV DELIMITER '^' HEADER;
copy ffequipfail_2013 from '$NFIRS_ROOT/ffequipfail.txt' CSV DELIMITER '^' HEADER;
EOF

# Assuming that you have GDAL installed & your DB is local & your mac/linux username has a trusted login to PostGIS
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_aa_2013.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_aa_2013_USA.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_ab_2013.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_ab_2013_USA.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_ac_2013.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_ac_2013_USA.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_ad_2013.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_ad_2013_USA.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_ae_2013.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_ae_2013_USA.shp
ogr2ogr -f PostgreSQL PG:dbname=$DB address_to_geo_ae_2013_St_Name.shp

# Data grooming so that the join to update geoms doesn't become an unmanagable mess
psql $DB << EOF
alter table incidentaddress_2013 add column id serial;
alter table incidentaddress add column source character varying(8) default '';

-- Consolidate everything in a single address_to_geo_2013 table

update address_to_geo_aa_2013 set street_pre = '' where street_pre is null;
update address_to_geo_aa_2013 set streettype = '' where streettype is null;
update address_to_geo_aa_2013 set streetsuf = '' where streetsuf is null;
update address_to_geo_aa_2013 set streetname = '' where streetname is null;
update address_to_geo_aa_2013 set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_aa_2013 alter column num_mile type character varying(8);
alter table address_to_geo_aa_2013 alter column zip5 type character varying(5);
alter table address_to_geo_aa_2013 alter column loc_type type character varying(1);
alter table address_to_geo_aa_2013 alter column apt_no type character varying(15);
update address_to_geo_aa_2013 set num_mile = '' where num_mile = '0';
update address_to_geo_aa_2013 set apt_no = '' where apt_no = '0';
update address_to_geo_aa_2013 set city_1 = '' where city_1 is null;
update address_to_geo_aa_2013 set state_id = '' where state_id is null;
update address_to_geo_aa_2013 set streetname = upper(streetname);
update address_to_geo_aa_2013 set streettype = upper(streettype);
update address_to_geo_aa_2013 set streetsuf = upper(streetsuf);
update address_to_geo_aa_2013 set street_pre = upper(street_pre);

update address_to_geo_aa_2013_usa set street_pre = '' where street_pre is null;
update address_to_geo_aa_2013_usa set streettype = '' where streettype is null;
update address_to_geo_aa_2013_usa set streetsuf = '' where streetsuf is null;
update address_to_geo_aa_2013_usa set streetname = '' where streetname is null;
update address_to_geo_aa_2013_usa set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_aa_2013_usa alter column num_mile type character varying(8);
alter table address_to_geo_aa_2013_usa alter column zip5 type character varying(5);
alter table address_to_geo_aa_2013_usa alter column loc_type type character varying(1);
alter table address_to_geo_aa_2013_usa alter column apt_no type character varying(15);
update address_to_geo_aa_2013_usa set num_mile = '' where num_mile = '0';
update address_to_geo_aa_2013_usa set apt_no = '' where apt_no = '0';
update address_to_geo_aa_2013_usa set city_1 = '' where city_1 is null;
update address_to_geo_aa_2013_usa set state_id = '' where state_id is null;
update address_to_geo_aa_2013_usa set streetname = upper(streetname);
update address_to_geo_aa_2013_usa set streettype = upper(streettype);
update address_to_geo_aa_2013_usa set streetsuf = upper(streetsuf);
update address_to_geo_aa_2013_usa set street_pre = upper(street_pre);

update address_to_geo_ab_2013 set street_pre = '' where street_pre is null;
update address_to_geo_ab_2013 set streettype = '' where streettype is null;
update address_to_geo_ab_2013 set streetsuf = '' where streetsuf is null;
update address_to_geo_ab_2013 set streetname = '' where streetname is null;
update address_to_geo_ab_2013 set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_ab_2013 alter column num_mile type character varying(8);
alter table address_to_geo_ab_2013 alter column zip5 type character varying(5);
alter table address_to_geo_ab_2013 alter column loc_type type character varying(1);
alter table address_to_geo_ab_2013 alter column apt_no type character varying(15);
update address_to_geo_ab_2013 set num_mile = '' where num_mile = '0';
update address_to_geo_ab_2013 set apt_no = '' where apt_no = '0';
update address_to_geo_ab_2013 set apt_no = '' where apt_no is null;
update address_to_geo_ab_2013 set city_1 = '' where city_1 is null;
update address_to_geo_ab_2013 set state_id = '' where state_id is null;
update address_to_geo_ab_2013 set streetname = upper(streetname);
update address_to_geo_ab_2013 set streettype = upper(streettype);
update address_to_geo_ab_2013 set streetsuf = upper(streetsuf);
update address_to_geo_ab_2013 set street_pre = upper(street_pre);

update address_to_geo_ab_2013_usa set street_pre = '' where street_pre is null;
update address_to_geo_ab_2013_usa set streettype = '' where streettype is null;
update address_to_geo_ab_2013_usa set streetsuf = '' where streetsuf is null;
update address_to_geo_ab_2013_usa set streetname = '' where streetname is null;
update address_to_geo_ab_2013_usa set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_ab_2013_usa alter column num_mile type character varying(8);
alter table address_to_geo_ab_2013_usa alter column zip5 type character varying(5);
alter table address_to_geo_ab_2013_usa alter column loc_type type character varying(1);
alter table address_to_geo_ab_2013_usa alter column apt_no type character varying(15);
update address_to_geo_ab_2013_usa set num_mile = '' where num_mile = '0';
update address_to_geo_ab_2013_usa set apt_no = '' where apt_no = '0';
update address_to_geo_ab_2013_usa set apt_no = '' where apt_no is null;
update address_to_geo_ab_2013_usa set city_1 = '' where city_1 is null;
update address_to_geo_ab_2013_usa set state_id = '' where state_id is null;
update address_to_geo_ab_2013_usa set streetname = upper(streetname);
update address_to_geo_ab_2013_usa set streettype = upper(streettype);
update address_to_geo_ab_2013_usa set streetsuf = upper(streetsuf);
update address_to_geo_ab_2013_usa set street_pre = upper(street_pre);

update address_to_geo_ac_2013 set street_pre = '' where street_pre is null;
update address_to_geo_ac_2013 set streettype = '' where streettype is null;
update address_to_geo_ac_2013 set streetsuf = '' where streetsuf is null;
update address_to_geo_ac_2013 set streetname = '' where streetname is null;
update address_to_geo_ac_2013 set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_ac_2013 alter column num_mile type character varying(8);
alter table address_to_geo_ac_2013 alter column zip5 type character varying(5);
alter table address_to_geo_ac_2013 alter column loc_type type character varying(1);
alter table address_to_geo_ac_2013 alter column apt_no type character varying(15);
update address_to_geo_ac_2013 set num_mile = '' where num_mile = '0';
update address_to_geo_ac_2013 set apt_no = '' where apt_no = '0';
update address_to_geo_ac_2013 set city_1 = '' where city_1 is null;
update address_to_geo_ac_2013 set state_id = '' where state_id is null;
update address_to_geo_ac_2013 set streetname = upper(streetname);
update address_to_geo_ac_2013 set streettype = upper(streettype);
update address_to_geo_ac_2013 set streetsuf = upper(streetsuf);
update address_to_geo_ac_2013 set street_pre = upper(street_pre);

update address_to_geo_ac_2013_usa set street_pre = '' where street_pre is null;
update address_to_geo_ac_2013_usa set streettype = '' where streettype is null;
update address_to_geo_ac_2013_usa set streetsuf = '' where streetsuf is null;
update address_to_geo_ac_2013_usa set streetname = '' where streetname is null;
update address_to_geo_ac_2013_usa set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_ac_2013_usa alter column num_mile type character varying(8);
alter table address_to_geo_ac_2013_usa alter column zip5 type character varying(5);
alter table address_to_geo_ac_2013_usa alter column loc_type type character varying(1);
alter table address_to_geo_ac_2013_usa alter column apt_no type character varying(15);
update address_to_geo_ac_2013_usa set num_mile = '' where num_mile = '0';
update address_to_geo_ac_2013_usa set apt_no = '' where apt_no = '0';
update address_to_geo_ac_2013_usa set city_1 = '' where city_1 is null;
update address_to_geo_ac_2013_usa set state_id = '' where state_id is null;
update address_to_geo_ac_2013_usa set streetname = upper(streetname);
update address_to_geo_ac_2013_usa set streettype = upper(streettype);
update address_to_geo_ac_2013_usa set streetsuf = upper(streetsuf);
update address_to_geo_ac_2013_usa set street_pre = upper(street_pre);

update address_to_geo_ad_2013 set street_pre = '' where street_pre is null;
update address_to_geo_ad_2013 set streettype = '' where streettype is null;
update address_to_geo_ad_2013 set streetsuf = '' where streetsuf is null;
update address_to_geo_ad_2013 set streetname = '' where streetname is null;
update address_to_geo_ad_2013 set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_ad_2013 alter column num_mile type character varying(8);
alter table address_to_geo_ad_2013 alter column zip5 type character varying(5);
alter table address_to_geo_ad_2013 alter column loc_type type character varying(1);
alter table address_to_geo_ad_2013 alter column apt_no type character varying(15);
update address_to_geo_ad_2013 set num_mile = '' where num_mile = '0';
update address_to_geo_ad_2013 set apt_no = '' where apt_no = '0';
update address_to_geo_ad_2013 set city_1 = '' where city_1 is null;
update address_to_geo_ad_2013 set state_id = '' where state_id is null;
update address_to_geo_ad_2013 set streetname = upper(streetname);
update address_to_geo_ad_2013 set streettype = upper(streettype);
update address_to_geo_ad_2013 set streetsuf = upper(streetsuf);
update address_to_geo_ad_2013 set street_pre = upper(street_pre);

update address_to_geo_ad_2013_usa set street_pre = '' where street_pre is null;
update address_to_geo_ad_2013_usa set streettype = '' where streettype is null;
update address_to_geo_ad_2013_usa set streetsuf = '' where streetsuf is null;
update address_to_geo_ad_2013_usa set streetname = '' where streetname is null;
update address_to_geo_ad_2013_usa set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_ad_2013_usa alter column num_mile type character varying(8);
alter table address_to_geo_ad_2013_usa alter column zip5 type character varying(5);
alter table address_to_geo_ad_2013_usa alter column loc_type type character varying(1);
alter table address_to_geo_ad_2013_usa alter column apt_no type character varying(15);
update address_to_geo_ad_2013_usa set num_mile = '' where num_mile = '0';
update address_to_geo_ad_2013_usa set apt_no = '' where apt_no = '0';
update address_to_geo_ad_2013_usa set city_1 = '' where city_1 is null;
update address_to_geo_ad_2013_usa set state_id = '' where state_id is null;
update address_to_geo_ad_2013_usa set streetname = upper(streetname);
update address_to_geo_ad_2013_usa set streettype = upper(streettype);
update address_to_geo_ad_2013_usa set streetsuf = upper(streetsuf);
update address_to_geo_ad_2013_usa set street_pre = upper(street_pre);

update address_to_geo_ae_2013 set street_pre = '' where street_pre is null;
update address_to_geo_ae_2013 set streettype = '' where streettype is null;
update address_to_geo_ae_2013 set streetsuf = '' where streetsuf is null;
update address_to_geo_ae_2013 set streetname = '' where streetname is null;
update address_to_geo_ae_2013 set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_ae_2013 alter column num_mile type character varying(8);
alter table address_to_geo_ae_2013 alter column zip5 type character varying(5);
alter table address_to_geo_ae_2013 alter column loc_type type character varying(1);
alter table address_to_geo_ae_2013 alter column apt_no type character varying(15);
update address_to_geo_ae_2013 set num_mile = '' where num_mile = '0';
update address_to_geo_ae_2013 set apt_no = '' where apt_no = '0';
update address_to_geo_ae_2013 set city_1 = '' where city_1 is null;
update address_to_geo_ae_2013 set state_id = '' where state_id is null;
update address_to_geo_ae_2013 set streetname = upper(streetname);
update address_to_geo_ae_2013 set streettype = upper(streettype);
update address_to_geo_ae_2013 set streetsuf = upper(streetsuf);
update address_to_geo_ae_2013 set street_pre = upper(street_pre);

update address_to_geo_ae_2013_usa set street_pre = '' where street_pre is null;
update address_to_geo_ae_2013_usa set streettype = '' where streettype is null;
update address_to_geo_ae_2013_usa set streetsuf = '' where streetsuf is null;
update address_to_geo_ae_2013_usa set streetname = '' where streetname is null;
update address_to_geo_ae_2013_usa set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_ae_2013_usa alter column num_mile type character varying(8);
alter table address_to_geo_ae_2013_usa alter column zip5 type character varying(5);
alter table address_to_geo_ae_2013_usa alter column loc_type type character varying(1);
alter table address_to_geo_ae_2013_usa alter column apt_no type character varying(15);
update address_to_geo_ae_2013_usa set num_mile = '' where num_mile = '0';
update address_to_geo_ae_2013_usa set apt_no = '' where apt_no = '0';
update address_to_geo_ae_2013_usa set city_1 = '' where city_1 is null;
update address_to_geo_ae_2013_usa set state_id = '' where state_id is null;
update address_to_geo_ae_2013_usa set streetname = upper(streetname);
update address_to_geo_ae_2013_usa set streettype = upper(streettype);
update address_to_geo_ae_2013_usa set streetsuf = upper(streetsuf);
update address_to_geo_ae_2013_usa set street_pre = upper(street_pre);

update address_to_geo_ae_2013_st_name set street_pre = '' where street_pre is null;
update address_to_geo_ae_2013_st_name set streettype = '' where streettype is null;
update address_to_geo_ae_2013_st_name set streetsuf = '' where streetsuf is null;
update address_to_geo_ae_2013_st_name set streetname = '' where streetname is null;
update address_to_geo_ae_2013_st_name set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table address_to_geo_ae_2013_st_name alter column num_mile type character varying(8);
alter table address_to_geo_ae_2013_st_name alter column zip5 type character varying(5);
alter table address_to_geo_ae_2013_st_name alter column loc_type type character varying(1);
alter table address_to_geo_ae_2013_st_name alter column apt_no type character varying(15);
update address_to_geo_ae_2013_st_name set num_mile = '' where num_mile = '0';
update address_to_geo_ae_2013_st_name set apt_no = '' where apt_no = '0';
update address_to_geo_ae_2013_st_name set city_1 = '' where city_1 is null;
update address_to_geo_ae_2013_st_name set state_id = '' where state_id is null;
update address_to_geo_ae_2013_st_name set streetname = upper(streetname);
update address_to_geo_ae_2013_st_name set streettype = upper(streettype);
update address_to_geo_ae_2013_st_name set streetsuf = upper(streetsuf);
update address_to_geo_ae_2013_st_name set street_pre = upper(street_pre);

alter table address_to_geo_aa_2013 drop column addnumfrom;
alter table address_to_geo_aa_2013 drop column side;
alter table address_to_geo_aa_2013 drop column addnumto;
alter table address_to_geo_aa_2013 drop column fdid;
alter table address_to_geo_aa_2013_usa drop column loc_name;
alter table address_to_geo_aa_2013_usa drop column addnumfrom;
alter table address_to_geo_aa_2013_usa drop column addnumto;
alter table address_to_geo_aa_2013_usa drop column staddr;
alter table address_to_geo_aa_2013_usa drop column rank;
alter table address_to_geo_aa_2013_usa drop column fdid;
alter table address_to_geo_aa_2013_usa drop column side;
alter table address_to_geo_aa_2013 add column source character varying (8) default 'AA';
alter table address_to_geo_aa_2013_usa add column source character varying (8) default 'AA_USA';

alter table address_to_geo_ab_2013 drop column addnumfrom;
alter table address_to_geo_ab_2013 drop column side;
alter table address_to_geo_ab_2013 drop column addnumto;
alter table address_to_geo_ab_2013 drop column fdid;
alter table address_to_geo_ab_2013_usa drop column loc_name;
alter table address_to_geo_ab_2013_usa drop column addnumfrom;
alter table address_to_geo_ab_2013_usa drop column addnumto;
alter table address_to_geo_ab_2013_usa drop column staddr;
alter table address_to_geo_ab_2013_usa drop column rank;
alter table address_to_geo_ab_2013_usa drop column fdid;
alter table address_to_geo_ab_2013_usa drop column side;
alter table address_to_geo_ab_2013 add column source character varying (8) default 'AB';
alter table address_to_geo_ab_2013_usa add column source character varying (8) default 'AB_USA';

alter table address_to_geo_ac_2013 drop column addnumfrom;
alter table address_to_geo_ac_2013 drop column side;
alter table address_to_geo_ac_2013 drop column addnumto;
alter table address_to_geo_ac_2013 drop column fdid;
alter table address_to_geo_ac_2013_usa drop column loc_name;
alter table address_to_geo_ac_2013_usa drop column addnumfrom;
alter table address_to_geo_ac_2013_usa drop column addnumto;
alter table address_to_geo_ac_2013_usa drop column staddr;
alter table address_to_geo_ac_2013_usa drop column rank;
alter table address_to_geo_ac_2013_usa drop column fdid;
alter table address_to_geo_ac_2013_usa drop column side;
alter table address_to_geo_ac_2013 add column source character varying (8) default 'AC';
alter table address_to_geo_ac_2013_usa add column source character varying (8) default 'AC_USA';

alter table address_to_geo_ad_2013 drop column addnumfrom;
alter table address_to_geo_ad_2013 drop column side;
alter table address_to_geo_ad_2013 drop column addnumto;
alter table address_to_geo_ad_2013 drop column fdid;
alter table address_to_geo_ad_2013_usa drop column loc_name;
alter table address_to_geo_ad_2013_usa drop column addnumfrom;
alter table address_to_geo_ad_2013_usa drop column addnumto;
alter table address_to_geo_ad_2013_usa drop column staddr;
alter table address_to_geo_ad_2013_usa drop column rank;
alter table address_to_geo_ad_2013_usa drop column fdid;
alter table address_to_geo_ad_2013_usa drop column side;
alter table address_to_geo_ad_2013 drop column field19;
alter table address_to_geo_ad_2013 drop column field20;
alter table address_to_geo_ad_2013 drop column field21;
alter table address_to_geo_ad_2013 drop column field22;
alter table address_to_geo_ad_2013 drop column field23;
alter table address_to_geo_ad_2013 drop column field24;
alter table address_to_geo_ad_2013 drop column field25;
alter table address_to_geo_ad_2013 drop column field26;
alter table address_to_geo_ad_2013 drop column field27;
alter table address_to_geo_ad_2013 drop column field28;
alter table address_to_geo_ad_2013 drop column field29;
alter table address_to_geo_ad_2013 drop column field30;
alter table address_to_geo_ad_2013 drop column field31;
alter table address_to_geo_ad_2013 drop column field32;
alter table address_to_geo_ad_2013 drop column field33;
alter table address_to_geo_ad_2013_usa drop column field19;
alter table address_to_geo_ad_2013_usa drop column field20;
alter table address_to_geo_ad_2013_usa drop column field21;
alter table address_to_geo_ad_2013_usa drop column field22;
alter table address_to_geo_ad_2013_usa drop column field23;
alter table address_to_geo_ad_2013_usa drop column field24;
alter table address_to_geo_ad_2013_usa drop column field25;
alter table address_to_geo_ad_2013_usa drop column field26;
alter table address_to_geo_ad_2013_usa drop column field27;
alter table address_to_geo_ad_2013_usa drop column field28;
alter table address_to_geo_ad_2013_usa drop column field29;
alter table address_to_geo_ad_2013_usa drop column field30;
alter table address_to_geo_ad_2013_usa drop column field31;
alter table address_to_geo_ad_2013_usa drop column field32;
alter table address_to_geo_ad_2013_usa drop column field33;
alter table address_to_geo_ad_2013 add column source character varying (8) default 'AD';
alter table address_to_geo_ad_2013_usa add column source character varying (8) default 'AD_USA';

alter table address_to_geo_ae_2013 drop column addnumfrom;
alter table address_to_geo_ae_2013 drop column side;
alter table address_to_geo_ae_2013 drop column addnumto;
alter table address_to_geo_ae_2013 drop column fdid;
alter table address_to_geo_ae_2013_usa drop column loc_name;
alter table address_to_geo_ae_2013_usa drop column addnumfrom;
alter table address_to_geo_ae_2013_usa drop column addnumto;
alter table address_to_geo_ae_2013_usa drop column staddr;
alter table address_to_geo_ae_2013_usa drop column rank;
alter table address_to_geo_ae_2013_usa drop column fdid;
alter table address_to_geo_ae_2013_usa drop column side;
alter table address_to_geo_ae_2013_st_name drop column fdid;
alter table address_to_geo_ae_2013_st_name alter column addnum
update address_to_geo_ae_2013_st_name set staddr = '';
alter table address_to_geo_ae_2013 add column source character varying (8) default 'AE';
alter table address_to_geo_ae_2013_usa add column source character varying (8) default 'AE_USA';
alter table address_to_geo_ae_2013_st_name add column source character varying (8) default 'AE_STNAM';

copy address_to_geo_aa_2013 to '/tmp/address_to_geo_aa_2013' DELIMITER ',';
copy address_to_geo_aa_2013_usa to '/tmp/address_to_geo_aa_2013_usa' DELIMITER ',' ;
copy address_to_geo_ab_2013 to '/tmp/address_to_geo_ab_2013' DELIMITER ',';
copy address_to_geo_ab_2013_usa to '/tmp/address_to_geo_ab_2013_usa' DELIMITER ',' ;
copy address_to_geo_ac_2013 to '/tmp/address_to_geo_ac_2013' DELIMITER ',';
copy address_to_geo_ac_2013_usa to '/tmp/address_to_geo_ac_2013_usa' DELIMITER ',' ;
copy address_to_geo_ad_2013 to '/tmp/address_to_geo_ad_2013' DELIMITER ',';
copy address_to_geo_ad_2013_usa to '/tmp/address_to_geo_ad_2013_usa' DELIMITER ',' ;
copy address_to_geo_ae_2013 to '/tmp/address_to_geo_ae_2013' DELIMITER ',';
copy address_to_geo_ae_2013_usa to '/tmp/address_to_geo_ae_2013_usa' DELIMITER ',' ;
copy address_to_geo_ae_2013_st_name to '/tmp/address_to_geo_ae_2013_st_name' DELIMITER ',' ;

create table address_to_geo_2013 as table address_to_geo_aa_2013 with no data;
copy address_to_geo_2013 from '/tmp/address_to_geo_aa_2013' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_aa_2013_usa' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_ab_2013' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_ab_2013_usa' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_ac_2013' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_ac_2013_usa' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_ad_2013' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_ad_2013_usa' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_ae_2013' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_ae_2013_usa' delimiter ',';
copy address_to_geo_2013 from '/tmp/address_to_geo_ae_2013_st_name' delimiter ',';

create index on address_to_geo_2013 (num_mile, upper(apt_no), upper(city_1), zip5, upper(street_pre), loc_type, upper(streetname), upper(streettype), upper(streetsuf), upper(state_id));
create index on address_to_geo_2013 (state_id);
create index on address_to_geo_2013 (source);
create index on incidentaddress_2013 (num_mile, upper(apt_no), upper(city), zip5, upper(street_pre), loc_type, upper(streetname), upper(streettype), upper(streetsuf), upper(state_id));
create index on incidentaddress_2013 (state_id);

-- Geocoding mashup

update incidentaddress_2013 as ia set geom = res.wkb_geometry, source = res.source
from (
select ia.id, aa.wkb_geometry, aa.source from address_to_geo_2013 aa inner join incidentaddress_2013 ia on (
aa.num_mile = ia.num_mile and
upper(aa.apt_no) = upper(ia.apt_no) and
upper(aa.city_1) = upper(ia.city) and
aa.zip5 = ia.zip5 and
upper(aa.street_pre) = upper(ia.street_pre) and
aa.loc_type = ia.loc_type and
upper(aa.streetname) = upper(ia.streetname) and
upper(aa.streettype) = upper(ia.streettype) and
upper(aa.streetsuf) = upper(ia.streetsuf) and
upper(aa.state_id) = upper(ia.state_id)
) where aa.num_mile != '' and score != 0 and aa.source in ('AA_USA', 'AB_USA', 'AC_USA', 'AD_USA', 'AE_USA', 'AE_STNAM')) as res
where ia.id = res.id -- 663718

update incidentaddress_2013 as ia set geom = res.wkb_geometry, source = res.source
from (
select ia.id, aa.wkb_geometry, aa.source from address_to_geo_2013 aa inner join incidentaddress_2013 ia on (
aa.num_mile = ia.num_mile and
upper(aa.apt_no) = upper(ia.apt_no) and
upper(aa.city_1) = upper(ia.city) and
aa.zip5 = ia.zip5 and
upper(aa.street_pre) = upper(ia.street_pre) and
aa.loc_type = ia.loc_type and
upper(aa.streetname) = upper(ia.streetname) and
upper(aa.streettype) = upper(ia.streettype) and
upper(aa.streetsuf) = upper(ia.streetsuf) and
upper(aa.state_id) = upper(ia.state_id)
) where aa.num_mile != '' and score != 0 and aa.source in ('AA', 'AB', 'AC', 'AD', 'AE')) as res
where ia.id = res.id -- 593590

-- % w/ geom by state
select (100.0 * sum(case when geom is null then 0 else 1 end) / count(1)) as percent_with_geom, state_id
from incidentaddress_2013 group by state_id order by percent_with_geom desc

-- % w/ geom total
select (100.0 * sum(case when geom is null then 0 else 1 end) / count(1)) as percent_with_geom
from incidentaddress_2013

-- 2013 data appending

-- 486689 pre - 526386 post
INSERT INTO arson(
            state, fdid, inc_date, inc_no, exp_no, version, case_stat, avail_mfi,
            mot_facts1, mot_facts2, mot_facts3, grp_invol1, grp_invol2, grp_invol3,
            entry_meth, ext_fire, devi_cont, devi_ignit, devi_fuel, inv_info1,
            inv_info2, inv_info3, inv_info4, inv_info5, inv_info6, inv_info7,
            inv_info8, prop_owner, init_ob1, init_ob2, init_ob3, init_ob4,
            init_ob5, init_ob6, init_ob7, init_ob8, lab_used1, lab_used2,
            lab_used3, lab_used4, lab_used5, lab_used6)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, case_stat, avail_mfi,
       mot_facts1, mot_facts2, mot_facts3, grp_invol1, grp_invol2, grp_invol3,
       entry_meth, ext_fire, devi_cont, devi_ignit, devi_fuel, inv_info1,
       inv_info2, inv_info3, inv_info4, inv_info5, inv_info6, inv_info7,
       inv_info8, prop_owner, init_ob1, init_ob2, init_ob3, init_ob4,
       init_ob5, init_ob6, init_ob7, init_ob8, lab_used1, lab_used2,
       lab_used3, lab_used4, lab_used5, lab_used6
  FROM arson_2013);

-- 299226 pre - 310659 post
INSERT INTO arsonagencyreferal(
            state, fdid, inc_date, inc_no, exp_no, agency_nam, version, ag_st_num,
            ag_st_pref, ag_street, ag_st_type, ag_st_suff, ag_apt_no, ag_city,
            ag_state, ag_zip5, ag_zip4, ag_phone, ag_case_no, ag_ori, ag_fid,
            ag_fdid)
    (SELECT state, fdid, inc_date, inc_no, exp_no, agency_nam, version, ag_st_num,
       ag_st_pref, ag_street, ag_st_type, ag_st_suff, ag_apt_no, ag_city,
       ag_state, ag_zip5, ag_zip4, ag_phone, ag_case_no, ag_ori, ag_fid,
       ag_fdid
  FROM arsonagencyreferal_2013);

-- 495820 pre - 500459 post
INSERT INTO arsonjuvsub(
            state, fdid, inc_date, inc_no, exp_no, sub_seq_no, version, age,
            gender, race, ethnicity, fam_type, risk_fact1, risk_fact2, risk_fact3,
            risk_fact4, risk_fact5, risk_fact6, risk_fact7, risk_fact8, juv_dispo)
    (SELECT state, fdid, inc_date, inc_no, exp_no, sub_seq_no, version, age,
       gender, race, ethnicity, fam_type, risk_fact1, risk_fact2, risk_fact3,
       risk_fact4, risk_fact5, risk_fact6, risk_fact7, risk_fact8, juv_dispo
  FROM arsonjuvsub_2013);

-- 3054542 pre - 3280569 post
INSERT INTO basicaid(
            state, fdid, inc_date, inc_no, exp_no, nfir_ver, fdidrecaid,
            fdidstrec, inc_nofdid)
    (SELECT state, fdid, inc_date, inc_no, exp_no, nfir_ver, fdidrecaid,
       fdidstrec, inc_nofdid
  FROM basicaid_2013);

-- 129889 pre - 140780 post
INSERT INTO civiliancasualty(
            state, fdid, inc_date, inc_no, exp_no, seq_number, version, gender,
            age, race, ethnicity, affiliat, inj_dt_tim, sev, cause_inj, hum_fact1,
            hum_fact2, hum_fact3, hum_fact4, hum_fact5, hum_fact6, hum_fact7,
            hum_fact8, fact_inj1, fact_inj2, fact_inj3, activ_inj, loc_inc,
            gen_loc_in, story_inc, story_inj, spc_loc_in, prim_symp, body_part,
            cc_dispos)
    (SELECT state, fdid, inc_date, inc_no, exp_no, seq_number, version, gender,
       age, race, ethnicity, affiliat, inj_dt_tim, sev, cause_inj, hum_fact1,
       hum_fact2, hum_fact3, hum_fact4, hum_fact5, hum_fact6, hum_fact7,
       hum_fact8, fact_inj1, fact_inj2, fact_inj3, activ_inj, loc_inc,
       gen_loc_in, story_inc, story_inj, spc_loc_in, prim_symp, body_part,
       cc_dispos
  FROM civiliancasualty_2013);

-- 6622 pre - 6622 post (appears to be a static table, same existing and same incoming, replacing EVERYTHING w/ the new values)
delete from codelookup;
INSERT INTO codelookup(
            fieldid, code_value, code_descr)
    (SELECT fieldid, code_value, code_descr
  FROM codelookup_2013);

-- 1288306 pre - 1291244 post
INSERT INTO ems(
            state, fdid, inc_date, inc_no, exp_no, patient_no, version, arrival,
            transport, provider_a, age, gender, race, eth_ems, hum_fact1,
            hum_fact2, hum_fact3, hum_fact4, hum_fact5, hum_fact6, hum_fact7,
            hum_fact8, other_fact, site_inj1, site_inj2, site_inj3, site_inj4,
            site_inj5, inj_type1, inj_type2, inj_type3, inj_type4, inj_type5,
            cause_ill, proc_use1, proc_use2, proc_use3, proc_use4, proc_use5,
            proc_use6, proc_use7, proc_use8, proc_use9, proc_use10, proc_use11,
            proc_use12, proc_use13, proc_use14, proc_use15, proc_use16, proc_use17,
            proc_use18, proc_use19, proc_use20, proc_use21, proc_use22, proc_use23,
            proc_use24, proc_use25, safe_eqp1, safe_eqp2, safe_eqp3, safe_eqp4,
            safe_eqp5, safe_eqp6, safe_eqp7, safe_eqp8, arrest, arr_des1,
            arr_des2, ar_rhythm, il_care, high_care, pat_status, pulse, ems_dispo)
    (SELECT state, fdid, inc_date, inc_no, exp_no, patient_no, version, arrival,
       transport, provider_a, age, gender, race, eth_ems, hum_fact1,
       hum_fact2, hum_fact3, hum_fact4, hum_fact5, hum_fact6, hum_fact7,
       hum_fact8, other_fact, site_inj1, site_inj2, site_inj3, site_inj4,
       site_inj5, inj_type1, inj_type2, inj_type3, inj_type4, inj_type5,
       cause_ill, proc_use1, proc_use2, proc_use3, proc_use4, proc_use5,
       proc_use6, proc_use7, proc_use8, proc_use9, proc_use10, proc_use11,
       proc_use12, proc_use13, proc_use14, proc_use15, proc_use16, proc_use17,
       proc_use18, proc_use19, proc_use20, proc_use21, proc_use22, proc_use23,
       proc_use24, proc_use25, safe_eqp1, safe_eqp2, safe_eqp3, safe_eqp4,
       safe_eqp5, safe_eqp6, safe_eqp7, safe_eqp8, arrest, arr_des1,
       arr_des2, ar_rhythm, il_care, high_care, pat_status, pulse, ems_dispo
  FROM ems_2013);

-- 39102 pre - 39385 post (<20 dupes)
INSERT INTO fdheader(
            state, fdid, fd_name, fd_str_no, fd_str_pre, fd_street, fd_str_typ,
            fd_str_suf, fd_city, fd_zip, fd_phone, fd_fax, fd_email, fd_fip_cty,
            no_station, no_pd_ff, no_vol_ff, no_vol_pdc)
    (SELECT distinct on (state, fdid) state, fdid, fd_name, fd_str_no, fd_str_pre, fd_street, fd_str_typ,
       fd_str_suf, fd_city, fd_zip, fd_phone, fd_fax, fd_email, fd_fip_cty,
       no_station, no_pd_ff, no_vol_ff, no_vol_pdc
  FROM fdheader_2013 where (state, fdid) not in (select state, fdid from fdheader));

-- 100435 pre - 109327 post
INSERT INTO ffcasualty(
            state, fdid, inc_date, inc_no, exp_no, ff_seq_no, version, gender,
            career, age, inj_date, responses, assignment, phys_cond, severity,
            taken_to, activity, symptom, pabi, cause, factor, object, wio,
            relation, story, location, vehicle, prot_eqp)
    (SELECT state, fdid, inc_date, inc_no, exp_no, ff_seq_no, version, gender,
       career, age, inj_date, responses, assignment, phys_cond, severity,
       taken_to, activity, symptom, pabi, cause, factor, object, wio,
       relation, story, location, vehicle, prot_eqp
  FROM ffcasualty_2013);

-- 34831 pre - 35597 post
INSERT INTO ffequipfail(
            state, fdid, inc_date, inc_no, exp_no, cas_seq_no, eqp_seq_no,
            version, equip_item, eqp_prob, eqp_man, eqp_mod, eqp_ser_no)
    (SELECT state, fdid, inc_date, inc_no, exp_no, cas_seq_no, eqp_seq_no,
       version, equip_item, eqp_prob, eqp_man, eqp_mod, eqp_ser_no
  FROM ffequipfail_2013);

-- 36514833 pre - 38518740 post
INSERT INTO basicincident(
            state, fdid, inc_date, inc_no, exp_no, version, dept_sta, inc_type,
            add_wild, aid, alarm, arrival, inc_cont, lu_clear, shift, alarms,
            district, act_tak1, act_tak2, act_tak3, app_mod, sup_app, ems_app,
            oth_app, sup_per, ems_per, oth_per, resou_aid, prop_loss, cont_loss,
            prop_val, cont_val, ff_death, oth_death, ff_inj, oth_inj, det_alert,
            haz_rel, mixed_use, prop_use, census)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, dept_sta, inc_type,
       add_wild, aid, alarm, arrival, inc_cont, lu_clear, shift, alarms,
       district, act_tak1, act_tak2, act_tak3, app_mod, sup_app, ems_app,
       oth_app, sup_per, ems_per, oth_per, resou_aid, prop_loss, cont_loss,
       prop_val, cont_val, ff_death, oth_death, ff_inj, oth_inj, det_alert,
       haz_rel, mixed_use, prop_use, census
  FROM basicincident_2013);

-- 6601273 pre - 7155944 post (many dups in fireincident_2013 ~500k)
INSERT INTO fireincident(
            state, fdid, inc_date, inc_no, exp_no, version, num_unit, not_res,
            bldg_invol, acres_burn, less_1acre, on_site_m1, mat_stor1, on_site_m2,
            mat_stor2, on_site_m3, mat_stor3, area_orig, heat_sourc, first_ign,
            conf_orig, type_mat, cause_ign, fact_ign_1, fact_ign_2, hum_fac_1,
            hum_fac_2, hum_fac_3, hum_fac_4, hum_fac_5, hum_fac_6, hum_fac_7,
            hum_fac_8, age, sex, equip_inv, sup_fac_1, sup_fac_2, sup_fac_3,
            mob_invol, mob_type, mob_make, mob_model, mob_year, mob_lic_pl,
            mob_state, mob_vin_no, eq_brand, eq_model, eq_ser_no, eq_year,
            eq_power, eq_port, fire_sprd, struc_type, struc_stat, bldg_above,
            bldg_below, bldg_lgth, bldg_width, tot_sq_ft, fire_orig, st_dam_min,
            st_dam_sig, st_dam_hvy, st_dam_xtr, flame_sprd, item_sprd, mat_sprd,
            detector, det_type, det_power, det_operat, det_effect, det_fail,
            aes_pres, aes_type, aes_oper, no_spr_op, aes_fail)
    (SELECT distinct on (state, fdid, inc_date, inc_no, exp_no) state, fdid, inc_date, inc_no, exp_no, version, num_unit, not_res,
       bldg_invol, acres_burn, less_1acre, on_site_m1, mat_stor1, on_site_m2,
       mat_stor2, on_site_m3, mat_stor3, area_orig, heat_sourc, first_ign,
       conf_orig, type_mat, cause_ign, fact_ign_1, fact_ign_2, hum_fac_1,
       hum_fac_2, hum_fac_3, hum_fac_4, hum_fac_5, hum_fac_6, hum_fac_7,
       hum_fac_8, age, sex, equip_inv, sup_fac_1, sup_fac_2, sup_fac_3,
       mob_invol, mob_type, mob_make, mob_model, mob_year, mob_lic_pl,
       mob_state, mob_vin_no, eq_brand, eq_model, eq_ser_no, eq_year,
       eq_power, eq_port, fire_sprd, struc_type, struc_stat, bldg_above,
       bldg_below, bldg_lgth, bldg_width, tot_sq_ft, fire_orig, st_dam_min,
       st_dam_sig, st_dam_hvy, st_dam_xtr, flame_sprd, item_sprd, mat_sprd,
       detector, det_type, det_power, det_operat, det_effect, det_fail,
       aes_pres, aes_type, aes_oper, no_spr_op, aes_fail
  FROM fireincident_2013);

-- 81122 pre - 98244 post
INSERT INTO hazchem(
            state, fdid, inc_date, inc_no, exp_no, seq_number, version, un_number,
            dot_class, cas_regis, chem_name, cont_type, cont_cap, cap_unit,
            amount_rel, units_rel, phys_state, rel_into)
    (SELECT state, fdid, inc_date, inc_no, exp_no, seq_number, version, un_number,
       dot_class, cas_regis, chem_name, cont_type, cont_cap, cap_unit,
       amount_rel, units_rel, phys_state, rel_into
  FROM hazchem_2013)

-- 109463 pre - 133173 post
INSERT INTO hazmat(
            state, fdid, inc_date, inc_no, exp_no, version, rel_from, rel_story,
            pop_dens, affec_meas, affec_unit, evac_meas, evac_unit, peop_evac,
            bldg_evac, haz_act1, haz_act2, haz_act3, occur_firs, cause_rel,
            fact_rel1, fact_rel2, fact_rel3, mit_fact1, mit_fact2, mit_fact3,
            eq_inv_rel, haz_dispo, haz_death, haz_inj)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, rel_from, rel_story,
       pop_dens, affec_meas, affec_unit, evac_meas, evac_unit, peop_evac,
       bldg_evac, haz_act1, haz_act2, haz_act3, occur_firs, cause_rel,
       fact_rel1, fact_rel2, fact_rel3, mit_fact1, mit_fact2, mit_fact3,
       eq_inv_rel, haz_dispo, haz_death, haz_inj
  FROM hazmat_2013);

-- 14544 pre - 38254 post
INSERT INTO hazmatequipinvolved(
            state, fdid, inc_date, inc_no, exp_no, version, eq_brand, eq_model,
            eq_ser_no, eq_year)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, eq_brand, eq_model,
       eq_ser_no, eq_year
  FROM hazmatequipinvolved_2013);

-- 34496 pre - 58206 post
INSERT INTO hazmobprop(
            state, fdid, inc_date, inc_no, exp_no, version, mp_type, mp_make,
            mp_model, mp_year, mp_license, mp_state, mp_dot_icc)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, mp_type, mp_make,
       mp_model, mp_year, mp_license, mp_state, mp_dot_icc
  FROM hazmobprop_2013);


-- 580964 pre - 625323 post
INSERT INTO wildlands(
            state, fdid, inc_date, inc_no, exp_no, version, latitude, longitude,
            township, north_sou, range, east_west, section, subsection, meridian,
            area_type, fire_cause, hum_fact1, hum_fact2, hum_fact3, hum_fact4,
            hum_fact5, hum_fact6, hum_fact7, hum_fact8, fact_ign1, fact_ign2,
            supp_fact1, supp_fact2, supp_fact3, heat_sourc, mob_prop, eq_inv_ign,
            nfdrs_id, weath_type, wind_dir, wind_speed, air_temp, rel_humid,
            fuel_moist, dangr_rate, bldg_inv, bldg_thr, acres_burn, crop_burn1,
            crop_burn2, crop_burn3, undet_burn, tax_burn, notax_burn, local_burn,
            couty_burn, st_burn, fed_burn, forei_burn, milit_burn, other_burn,
            prop_manag, fed_code, nfdrs_fm, person_fir, gender, age, activity_w,
            horiz_dis, type_row, elevation, pos_slope, aspect, flame_lgth,
            spread_rat)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, latitude, longitude,
       township, north_sou, range, east_west, section, subsection, meridian,
       area_type, fire_cause, hum_fact1, hum_fact2, hum_fact3, hum_fact4,
       hum_fact5, hum_fact6, hum_fact7, hum_fact8, fact_ign1, fact_ign2,
       supp_fact1, supp_fact2, supp_fact3, heat_sourc, mob_prop, eq_inv_ign,
       nfdrs_id, weath_type, wind_dir, wind_speed, air_temp, rel_humid,
       fuel_moist, dangr_rate, bldg_inv, bldg_thr, acres_burn, crop_burn1,
       crop_burn2, crop_burn3, undet_burn, tax_burn, notax_burn, local_burn,
       couty_burn, st_burn, fed_burn, forei_burn, milit_burn, other_burn,
       prop_manag, fed_code, nfdrs_fm, person_fir, gender, age, activity_w,
       horiz_dis, type_row, elevation, pos_slope, aspect, flame_lgth,
       spread_rat
  FROM wildlands_2013);

INSERT INTO incidentaddress(
            state, fdid, inc_date, inc_no, exp_no, loc_type, num_mile, street_pre,
            streetname, streettype, streetsuf, apt_no, city, state_id, zip5,
            zip4, x_street, addid, addid_try, geom, bkgpidfp00, bkgpidfp10, source)
    (SELECT state, fdid, inc_date, inc_no, exp_no, loc_type, num_mile, street_pre,
       streetname, streettype, streetsuf, apt_no, city, state_id, zip5,
       zip4, x_street, addid, addid_try, geom, bkgpidfp00, bkgpidfp10, source
  FROM incidentaddress_2013);

-- We can clean out all of the 2013 tables now that the data has been appended
drop table fireincident_2013;
drop table hazchem_2013;
drop table hazmat_2013;
drop table hazmatequipinvolved_2013;
drop table hazmobprop_2013;
drop table incidentaddress_2013;
drop table wildlands_2013;
drop table arson_2013;
drop table arsonagencyreferal_2013;
drop table arsonjuvsub_2013;
drop table basicaid_2013;
drop table basicincident_2013;
drop table civiliancasualty_2013;
drop table codelookup_2013;
drop table ems_2013;
drop table fdheader_2013;
drop table ffcasualty_2013;
drop table ffequipfail_2013;

alter table buildingfires rename to buildingfires_prior_to_2013;

-- Run the building_fires.sql script

-- Move to geocoding_2013 schema for archival purposes

create schema if not exists geocoding_2013;
alter table address_to_geo_2013 set schema geocoding_2013;
alter table address_to_geo_aa_2013 set schema geocoding_2013;
alter table address_to_geo_aa_2013_usa set schema geocoding_2013;
alter table address_to_geo_ab_2013 set schema geocoding_2013;
alter table address_to_geo_ab_2013_usa set schema geocoding_2013;
alter table address_to_geo_ac_2013 set schema geocoding_2013;
alter table address_to_geo_ac_2013_usa set schema geocoding_2013;
alter table address_to_geo_ad_2013 set schema geocoding_2013;
alter table address_to_geo_ad_2013_usa set schema geocoding_2013;
alter table address_to_geo_ae_2013 set schema geocoding_2013;
alter table address_to_geo_ae_2013_usa set schema geocoding_2013;
alter table address_to_geo_ae_2013_st_name set schema geocoding_2013;


EOF

pg_dump --no-owner nfirs_dev > firecares_nfirs_7_14_2015
gzip firecares_nfirs_7_14_2015 > firecares_nfirs_7_14_2015.gz
