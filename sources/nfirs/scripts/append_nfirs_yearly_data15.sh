#!/bin/sh

# OVERVIEW:
# The purpose of this collection of scripts is to load the NFIRS yearly data
# into a dump of the existing Firecares Database.  The individual 18 incident types
# (fireincident, hazchem, hazmat, etc) are loaded into temp tables and then
# appended to the master "fireincident", etc tables.  The bulk of this script
# has to do w/ matching addresses in geocoding results to the new incidents in
# the "incidentaddress_${YEAR}" and appending that to the "incidentaddress" table.
# The incident_address_${YEAR}_aa/ab/ac/ad/ae tables contain the geocoding information
# from shapefiles and will be used to augment the incidentaddress_${YEAR}
# table's records with geometries.


# Assumes that the assocated shapefiles are alongside the NFIRS data

DB=nfirs15
YEAR=2015

psql $DB << EOF
create table fireincident_$YEAR as table fireincident with no data;
create table hazchem_$YEAR as table hazchem with no data;
create table hazmat_$YEAR as table hazmat with no data;
create table hazmatequipinvolved_$YEAR as table hazmatequipinvolved with no data;
create table hazmobprop_$YEAR as table hazmobprop with no data;
create table incidentaddress_$YEAR as table incidentaddress with no data;
create table wildlands_$YEAR as table wildlands with no data;
create table arson_$YEAR as table arson with no data;
create table arsonagencyreferal_$YEAR as table arsonagencyreferal with no data;
create table arsonjuvsub_$YEAR as table arsonjuvsub with no data;
create table basicaid_$YEAR as table basicaid with no data;
create table basicincident_$YEAR as table basicincident with no data;
create table civiliancasualty_$YEAR as table civiliancasualty with no data;
create table codelookup_$YEAR as table codelookup with no data;
create table ems_$YEAR as table ems with no data;
create table fdheader_$YEAR as table fdheader with no data;
create table ffcasualty_$YEAR as table ffcasualty with no data;
create table ffequipfail_$YEAR as table ffequipfail with no data;
EOF


# convert from MMDDYYYY to something that Postgres can handle MM/DD/YYYY
LANG=C sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' fireincident.txt
LANG=C sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' incidentaddress.txt
LANG=C sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' arsonjuvsub.txt
LANG=C sed -i.bak 's/\"\([U123]\) -- [^"]*\"/"\1"/' arsonjuvsub.txt
LANG=C sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' basicincident.txt
# convert from MMDDYYYYHHmm to something that Postgres can handle (MM/DD/YYYY HH:mm), matching the closing ^ causes adjacent fields to NOT be matched
LANG=C sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\([0-9]\{2\}\)\([0-9]\{2\}\)/^"\1\/\2\/\3 \4:\5"/g' basicincident.txt
LANG=C sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' civiliancasualty.txt
LANG=C sed -i.bak 's/\^\([0-9]\{2\}\)\([0-9]\{2\}\)\([0-9]\{4\}\)\^/^"\1\/\2\/\3"^/' ffcasualty.txt
# found one \x00 byte, vim => :%s/[\x00]//g OR the tr below
cp incidentaddress.txt incidentaddress.prereplace.txt && LANG=C tr < incidentaddress.txt -d '\000' > incidentaddress2.txt && mv incidentaddress2.txt incidentaddress.txt
# fdheader riddled w/ \x00 bytes, vim => :%s/[\x00]//g OR the tr below
cp fdheader.txt fdheader.prereplace.txt && LANG=C tr < fdheader.txt -d '\000' > fdheader2.txt && mv fdheader2.txt fdheader.txt

# looks like the dumps are ASCII, but have some utf-8 encoded characters, force LATIN1 to load w/o having automagic UTF-8 decoding issues
psql $DB << EOF
set client_encoding = 'LATIN1';
copy fireincident_$YEAR from '`pwd`/fireincident.txt' CSV DELIMITER '^' HEADER;
copy hazchem_$YEAR from '`pwd`/hazchem.txt' CSV DELIMITER '^' HEADER;
copy hazmat_$YEAR from '`pwd`/hazmat.txt' CSV DELIMITER '^' HEADER;
copy hazmatequipinvolved_$YEAR from '`pwd`/hazmatequipinvolved.txt' CSV DELIMITER '^' HEADER;
copy hazmobprop_$YEAR from '`pwd`/hazmobprop.txt' CSV DELIMITER '^' HEADER;
copy incidentaddress_$YEAR (state, fdid, inc_date, inc_no, exp_no, loc_type, num_mile, street_pre, streetname, streettype, streetsuf, apt_no, city, state_id, zip5, zip4, x_street) from '`pwd`/incidentaddress.txt' CSV DELIMITER '^' HEADER;
copy wildlands_$YEAR from '`pwd`/wildlands.txt' CSV DELIMITER '^' HEADER;
copy arson_$YEAR from '`pwd`/arson.txt' CSV DELIMITER '^' HEADER;
copy arsonagencyreferal_$YEAR from '`pwd`/arsonagencyreferal.txt' CSV DELIMITER '^' HEADER;
copy arsonjuvsub_$YEAR from '`pwd`/arsonjuvsub.txt' CSV DELIMITER '^' HEADER;
copy basicaid_$YEAR from '`pwd`/basicaid.txt' CSV DELIMITER '^' HEADER;
copy basicincident_$YEAR from '`pwd`/basicincident.txt' CSV DELIMITER '^' HEADER;
copy civiliancasualty_$YEAR from '`pwd`/civiliancasualty.txt' CSV DELIMITER '^' HEADER;
copy codelookup_$YEAR from '`pwd`/codelookup.txt' CSV DELIMITER '^' HEADER;
copy ems_$YEAR from '`pwd`/ems.txt' CSV DELIMITER '^' HEADER;
copy fdheader_$YEAR from '`pwd`/fdheader.txt' CSV DELIMITER '^' HEADER;
copy ffcasualty_$YEAR (state, fdid, inc_date, inc_no, exp_no, ff_seq_no, version, gender, career, age, inj_date, responses, assignment, phys_cond, severity, taken_to, activity, symptom, pabi, cause, factor, object, wio, relation, story, location, vehicle, prot_eqp) from '`pwd`/ffcasualty.txt' CSV DELIMITER '^' HEADER;
copy ffequipfail_$YEAR from '`pwd`/ffequipfail.txt' CSV DELIMITER '^' HEADER;

EOF

# Assuming that you have GDAL installed & your DB is local & your mac/linux username has a trusted login to PostGIS
### no dbffile ogr2ogr -f PostgreSQL PG:dbname=nfirs15 incident_address_ab_2015_p2_042017_USA_loc.shp
# shp2pgsql -s 4326 incident_address_aa_2015_p2_042017_USA_loc public.incident_address_aa_2015_p2_042017_USA_loc | psql -h localhost -U firecares -W password -d nfirs15
# not working ogr2ogr -f PostgreSQL PG:"host=localhost user=firecares dbname=nfirs15 password=password" -nlt GEOMETRY incident_address_aa_2015_p2_042017_USA_loc.shp
# using a conversion from web mercator to wgs84
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_aa_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ab_2015_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ac_2015_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ac_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ad_2015_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ad_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ae_2015_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ae_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_af_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ag_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ah_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ai_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_aj_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ak_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_al_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_am_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_an_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ao_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ap_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_aq_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ar_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_as_2015_p2_042017_USA_loc.shp

ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_at_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_au_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_av_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_aw_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ax_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ay_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_az_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_az_v2_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_ba_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bb_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bc_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bd_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_be_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bf_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bg_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bh_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bi_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bj_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bk_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bl_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bm_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bn_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bo_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bp_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bq_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_br_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bs_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bt_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bu_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bv_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bw_2015_p2_042017_USA_loc.shp
ogr2ogr -f PostgreSQL -s_srs EPSG:3857 -t_srs EPSG:4326 PG:dbname=nfirs15 incident_address_bx_2015_p2_042017_USA_loc.shp

# Data grooming so that the join to update geoms doesn't become an unmanagable mess
psql $DB << EOF
alter table incidentaddress_${YEAR} add column id serial;
alter table incidentaddress add column source character varying(8) default '';

-- Consolidate everything in a single address_to_geo table

--TODO: Here right now

update incident_address_${YEAR}_aa set street_pre = '' where street_pre is null;
update incident_address_${YEAR}_aa set streettype = '' where streettype is null;
update incident_address_${YEAR}_aa set streetsuf = '' where streetsuf is null;
update incident_address_${YEAR}_aa set streetname = '' where streetname is null;
update incident_address_${YEAR}_aa set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table incident_address_${YEAR}_aa alter column num_mile type character varying(8);
alter table incident_address_${YEAR}_aa alter column zip5 type character varying(5);
alter table incident_address_${YEAR}_aa alter column loc_type type character varying(1);
alter table incident_address_${YEAR}_aa alter column apt_no type character varying(15);
update incident_address_${YEAR}_aa set num_mile = '' where num_mile = '0';
update incident_address_${YEAR}_aa set apt_no = '' where apt_no = '0';
update incident_address_${YEAR}_aa set city_1 = '' where city_1 is null;
update incident_address_${YEAR}_aa set state_id = '' where state_id is null;
update incident_address_${YEAR}_aa set streetname = upper(streetname);
update incident_address_${YEAR}_aa set streettype = upper(streettype);
update incident_address_${YEAR}_aa set streetsuf = upper(streetsuf);
update incident_address_${YEAR}_aa set street_pre = upper(street_pre);

update incident_address_${YEAR}_ab set street_pre = '' where street_pre is null;
update incident_address_${YEAR}_ab set streettype = '' where streettype is null;
update incident_address_${YEAR}_ab set streetsuf = '' where streetsuf is null;
update incident_address_${YEAR}_ab set streetname = '' where streetname is null;
update incident_address_${YEAR}_ab set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table incident_address_${YEAR}_ab alter column num_mile type character varying(8);
alter table incident_address_${YEAR}_ab alter column zip5 type character varying(5);
alter table incident_address_${YEAR}_ab alter column loc_type type character varying(1);
alter table incident_address_${YEAR}_ab alter column apt_no type character varying(15);
update incident_address_${YEAR}_ab set num_mile = '' where num_mile = '0';
update incident_address_${YEAR}_ab set apt_no = '' where apt_no = '0';
update incident_address_${YEAR}_ab set apt_no = '' where apt_no is null;
update incident_address_${YEAR}_ab set city_1 = '' where city_1 is null;
update incident_address_${YEAR}_ab set state_id = '' where state_id is null;
update incident_address_${YEAR}_ab set streetname = upper(streetname);
update incident_address_${YEAR}_ab set streettype = upper(streettype);
update incident_address_${YEAR}_ab set streetsuf = upper(streetsuf);
update incident_address_${YEAR}_ab set street_pre = upper(street_pre);

update incident_address_${YEAR}_ac set street_pre = '' where street_pre is null;
update incident_address_${YEAR}_ac set streettype = '' where streettype is null;
update incident_address_${YEAR}_ac set streetsuf = '' where streetsuf is null;
update incident_address_${YEAR}_ac set streetname = '' where streetname is null;
update incident_address_${YEAR}_ac set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table incident_address_${YEAR}_ac alter column num_mile type character varying(8);
alter table incident_address_${YEAR}_ac alter column zip5 type character varying(5);
alter table incident_address_${YEAR}_ac alter column loc_type type character varying(1);
alter table incident_address_${YEAR}_ac alter column apt_no type character varying(15);
update incident_address_${YEAR}_ac set num_mile = '' where num_mile = '0';
update incident_address_${YEAR}_ac set apt_no = '' where apt_no = '0';
update incident_address_${YEAR}_ac set city_1 = '' where city_1 is null;
update incident_address_${YEAR}_ac set state_id = '' where state_id is null;
update incident_address_${YEAR}_ac set streetname = upper(streetname);
update incident_address_${YEAR}_ac set streettype = upper(streettype);
update incident_address_${YEAR}_ac set streetsuf = upper(streetsuf);
update incident_address_${YEAR}_ac set street_pre = upper(street_pre);

update incident_address_${YEAR}_ad set street_pre = '' where street_pre is null;
update incident_address_${YEAR}_ad set streettype = '' where streettype is null;
update incident_address_${YEAR}_ad set streetsuf = '' where streetsuf is null;
update incident_address_${YEAR}_ad set streetname = '' where streetname is null;
update incident_address_${YEAR}_ad set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table incident_address_${YEAR}_ad alter column num_mile type character varying(8);
alter table incident_address_${YEAR}_ad alter column zip5 type character varying(5);
alter table incident_address_${YEAR}_ad alter column loc_type type character varying(1);
alter table incident_address_${YEAR}_ad alter column apt_no type character varying(15);
update incident_address_${YEAR}_ad set num_mile = '' where num_mile = '0';
update incident_address_${YEAR}_ad set apt_no = '' where apt_no = '0';
update incident_address_${YEAR}_ad set city_1 = '' where city_1 is null;
update incident_address_${YEAR}_ad set state_id = '' where state_id is null;
update incident_address_${YEAR}_ad set streetname = upper(streetname);
update incident_address_${YEAR}_ad set streettype = upper(streettype);
update incident_address_${YEAR}_ad set streetsuf = upper(streetsuf);
update incident_address_${YEAR}_ad set street_pre = upper(street_pre);

update incident_address_${YEAR}_ae set street_pre = '' where street_pre is null;
update incident_address_${YEAR}_ae set streettype = '' where streettype is null;
update incident_address_${YEAR}_ae set streetsuf = '' where streetsuf is null;
update incident_address_${YEAR}_ae set streetname = '' where streetname is null;
update incident_address_${YEAR}_ae set zip5 = to_number(postal, '99999') where zip5 = 0;
alter table incident_address_${YEAR}_ae alter column num_mile type character varying(8);
alter table incident_address_${YEAR}_ae alter column zip5 type character varying(5);
alter table incident_address_${YEAR}_ae alter column loc_type type character varying(1);
alter table incident_address_${YEAR}_ae alter column apt_no type character varying(15);
update incident_address_${YEAR}_ae set num_mile = '' where num_mile = '0';
update incident_address_${YEAR}_ae set apt_no = '' where apt_no = '0';
update incident_address_${YEAR}_ae set city_1 = '' where city_1 is null;
update incident_address_${YEAR}_ae set state_id = '' where state_id is null;
update incident_address_${YEAR}_ae set streetname = upper(streetname);
update incident_address_${YEAR}_ae set streettype = upper(streettype);
update incident_address_${YEAR}_ae set streetsuf = upper(streetsuf);
update incident_address_${YEAR}_ae set street_pre = upper(street_pre);


alter table incident_address_${YEAR}_aa drop column addnumfrom;
alter table incident_address_${YEAR}_aa drop column side;
alter table incident_address_${YEAR}_aa drop column addnumto;
alter table incident_address_${YEAR}_aa drop column fdid;
alter table incident_address_${YEAR}_aa add column source character varying (8) default 'AA';

alter table incident_address_${YEAR}_ab drop column addnumfrom;
alter table incident_address_${YEAR}_ab drop column side;
alter table incident_address_${YEAR}_ab drop column addnumto;
alter table incident_address_${YEAR}_ab drop column fdid;
alter table incident_address_${YEAR}_ab add column source character varying (8) default 'AB';

alter table incident_address_${YEAR}_ac drop column addnumfrom;
alter table incident_address_${YEAR}_ac drop column side;
alter table incident_address_${YEAR}_ac drop column addnumto;
alter table incident_address_${YEAR}_ac drop column fdid;
alter table incident_address_${YEAR}_ac add column source character varying (8) default 'AC';

alter table incident_address_${YEAR}_ad drop column addnumfrom;
alter table incident_address_${YEAR}_ad drop column side;
alter table incident_address_${YEAR}_ad drop column addnumto;
alter table incident_address_${YEAR}_ad drop column fdid;
alter table incident_address_${YEAR}_ad drop column field19;
alter table incident_address_${YEAR}_ad drop column field20;
alter table incident_address_${YEAR}_ad drop column field21;
alter table incident_address_${YEAR}_ad drop column field22;
alter table incident_address_${YEAR}_ad drop column field23;
alter table incident_address_${YEAR}_ad drop column field24;
alter table incident_address_${YEAR}_ad drop column field25;
alter table incident_address_${YEAR}_ad drop column field26;
alter table incident_address_${YEAR}_ad drop column field27;
alter table incident_address_${YEAR}_ad drop column field28;
alter table incident_address_${YEAR}_ad drop column field29;
alter table incident_address_${YEAR}_ad drop column field30;
alter table incident_address_${YEAR}_ad drop column field31;
alter table incident_address_${YEAR}_ad drop column field32;
alter table incident_address_${YEAR}_ad drop column field33;
alter table incident_address_${YEAR}_ad add column source character varying (8) default 'AD';

alter table incident_address_${YEAR}_ae drop column addnumfrom;
alter table incident_address_${YEAR}_ae drop column side;
alter table incident_address_${YEAR}_ae drop column addnumto;
alter table incident_address_${YEAR}_ae drop column fdid;
alter table incident_address_${YEAR}_ae add column source character varying (8) default 'AE';

copy incident_address_${YEAR}_aa to '/tmp/address_to_geo_aa' DELIMITER ',';
copy incident_address_${YEAR}_ab to '/tmp/address_to_geo_ab' DELIMITER ',';
copy incident_address_${YEAR}_ac to '/tmp/address_to_geo_ac' DELIMITER ',';
copy incident_address_${YEAR}_ad to '/tmp/address_to_geo_ad' DELIMITER ',';
copy incident_address_${YEAR}_ae to '/tmp/address_to_geo_ae' DELIMITER ',';

create table address_to_geo_${YEAR} as table incident_address_${YEAR}_aa with no data;
copy address_to_geo_${YEAR} from '/tmp/address_to_geo_aa' delimiter ',';
copy address_to_geo_${YEAR} from '/tmp/address_to_geo_ab' delimiter ',';
copy address_to_geo_${YEAR} from '/tmp/address_to_geo_ac' delimiter ',';
copy address_to_geo_${YEAR} from '/tmp/address_to_geo_ad' delimiter ',';
copy address_to_geo_${YEAR} from '/tmp/address_to_geo_ae' delimiter ',';

create index on address_to_geo_${YEAR} (num_mile, upper(apt_no), upper(city_1), zip5, upper(street_pre), loc_type, upper(streetname), upper(streettype), upper(streetsuf), upper(state_id));
create index on address_to_geo_${YEAR} (state_id);
create index on address_to_geo_${YEAR} (source);
create index on incidentaddress_${YEAR} (num_mile, upper(apt_no), upper(city), zip5, upper(street_pre), loc_type, upper(streetname), upper(streettype), upper(streetsuf), upper(state_id));
create index on incidentaddress_${YEAR} (state_id);

-- Geocoding mashup

/* update incidentaddress_${YEAR} as ia set geom = res.wkb_geometry, source = res.source
from (
select ia.id, aa.wkb_geometry, aa.source from address_to_geo_${YEAR} aa inner join incidentaddress_${YEAR} ia on (
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
where ia.id = res.id -- 663718 */

update incidentaddress_${YEAR} as ia set geom = res.wkb_geometry, source = res.source
from (
select ia.id, aa.wkb_geometry, aa.source from address_to_geo_${YEAR} aa inner join incidentaddress_${YEAR} ia on (
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
select (100.0 * sum(case when geom is null then 0 else 1 end) / count(1)) as percent_with_geom, state_id, count(state_id)
from incidentaddress_${YEAR} group by state_id order by percent_with_geom desc;

-- % w/ geom total
select (100.0 * sum(case when geom is null then 0 else 1 end) / count(1)) as percent_with_geom
from incidentaddress_${YEAR};

-- data appending for the current year

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
  FROM arson_${YEAR});

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
  FROM arsonagencyreferal_${YEAR});

-- 495820 pre - 500459 post
INSERT INTO arsonjuvsub(
            state, fdid, inc_date, inc_no, exp_no, sub_seq_no, version, age,
            gender, race, ethnicity, fam_type, risk_fact1, risk_fact2, risk_fact3,
            risk_fact4, risk_fact5, risk_fact6, risk_fact7, risk_fact8, juv_dispo)
    (SELECT state, fdid, inc_date, inc_no, exp_no, sub_seq_no, version, age,
       gender, race, ethnicity, fam_type, risk_fact1, risk_fact2, risk_fact3,
       risk_fact4, risk_fact5, risk_fact6, risk_fact7, risk_fact8, juv_dispo
  FROM arsonjuvsub_${YEAR});

-- 3054542 pre - 3280569 post
INSERT INTO basicaid(
            state, fdid, inc_date, inc_no, exp_no, nfir_ver, fdidrecaid,
            fdidstrec, inc_nofdid)
    (SELECT state, fdid, inc_date, inc_no, exp_no, nfir_ver, fdidrecaid,
       fdidstrec, inc_nofdid
  FROM basicaid_${YEAR});

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
  FROM civiliancasualty_${YEAR});

-- 6622 pre - 6622 post (appears to be a static table, same existing and same incoming, replacing EVERYTHING w/ the new values)
delete from codelookup;
INSERT INTO codelookup(
            fieldid, code_value, code_descr)
    (SELECT fieldid, code_value, code_descr
  FROM codelookup_${YEAR});

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
  FROM ems_${YEAR});

-- 39102 pre - 39385 post (<20 dupes)
INSERT INTO fdheader(
            state, fdid, fd_name, fd_str_no, fd_str_pre, fd_street, fd_str_typ,
            fd_str_suf, fd_city, fd_zip, fd_phone, fd_fax, fd_email, fd_fip_cty,
            no_station, no_pd_ff, no_vol_ff, no_vol_pdc)
    (SELECT distinct on (state, fdid) state, fdid, fd_name, fd_str_no, fd_str_pre, fd_street, fd_str_typ,
       fd_str_suf, fd_city, fd_zip, fd_phone, fd_fax, fd_email, fd_fip_cty,
       no_station, no_pd_ff, no_vol_ff, no_vol_pdc
  FROM fdheader_${YEAR} where (state, fdid) not in (select state, fdid from fdheader));

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
  FROM ffcasualty_${YEAR});

-- 34831 pre - 35597 post
INSERT INTO ffequipfail(
            state, fdid, inc_date, inc_no, exp_no, cas_seq_no, eqp_seq_no,
            version, equip_item, eqp_prob, eqp_man, eqp_mod, eqp_ser_no)
    (SELECT state, fdid, inc_date, inc_no, exp_no, cas_seq_no, eqp_seq_no,
       version, equip_item, eqp_prob, eqp_man, eqp_mod, eqp_ser_no
  FROM ffequipfail_${YEAR});

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
  FROM basicincident_${YEAR});

UPDATE fireincident_${YEAR} set fire_sprd=null where fire_sprd='';

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
  FROM fireincident_${YEAR});

-- 81122 pre - 98244 post
INSERT INTO hazchem(
            state, fdid, inc_date, inc_no, exp_no, seq_number, version, un_number,
            dot_class, cas_regis, chem_name, cont_type, cont_cap, cap_unit,
            amount_rel, units_rel, phys_state, rel_into)
    (SELECT state, fdid, inc_date, inc_no, exp_no, seq_number, version, un_number,
       dot_class, cas_regis, chem_name, cont_type, cont_cap, cap_unit,
       amount_rel, units_rel, phys_state, rel_into
  FROM hazchem_${YEAR})

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
  FROM hazmat_${YEAR});

-- 14544 pre - 38254 post
INSERT INTO hazmatequipinvolved(
            state, fdid, inc_date, inc_no, exp_no, version, eq_brand, eq_model,
            eq_ser_no, eq_year)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, eq_brand, eq_model,
       eq_ser_no, eq_year
  FROM hazmatequipinvolved_${YEAR});

-- 34496 pre - 58206 post
INSERT INTO hazmobprop(
            state, fdid, inc_date, inc_no, exp_no, version, mp_type, mp_make,
            mp_model, mp_year, mp_license, mp_state, mp_dot_icc)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, mp_type, mp_make,
       mp_model, mp_year, mp_license, mp_state, mp_dot_icc
  FROM hazmobprop_${YEAR});


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
  FROM wildlands_${YEAR});

INSERT INTO incidentaddress(
            state, fdid, inc_date, inc_no, exp_no, loc_type, num_mile, street_pre,
            streetname, streettype, streetsuf, apt_no, city, state_id, zip5,
            zip4, x_street, addid, addid_try, geom, bkgpidfp00, bkgpidfp10, source)
    (SELECT state, fdid, inc_date, inc_no, exp_no, loc_type, num_mile, street_pre,
       streetname, streettype, streetsuf, apt_no, city, state_id, zip5,
       zip4, x_street, addid, addid_try, geom, bkgpidfp00, bkgpidfp10, source
  FROM incidentaddress_${YEAR});

-- We can clean out all of the year tables now that the data has been appended
drop table fireincident_${YEAR};
drop table hazchem_${YEAR};
drop table hazmat_${YEAR};
drop table hazmatequipinvolved_${YEAR};
drop table hazmobprop_${YEAR};
drop table incidentaddress_${YEAR};
drop table wildlands_${YEAR};
drop table arson_${YEAR};
drop table arsonagencyreferal_${YEAR};
drop table arsonjuvsub_${YEAR};
drop table basicaid_${YEAR};
drop table basicincident_${YEAR};
drop table civiliancasualty_${YEAR};
drop table codelookup_${YEAR};
drop table ems_${YEAR};
drop table fdheader_${YEAR};
drop table ffcasualty_${YEAR};
drop table ffequipfail_${YEAR};

alter table buildingfires rename to buildingfires_prior_to_${YEAR};

-- Run the building_fires.sql script

-- Move to geocoding_${YEAR} schema for archival purposes

create schema if not exists geocoding_${YEAR};
alter table address_to_geo_${YEAR} set schema geocoding_${YEAR};
alter table address_to_geo_aa_${YEAR} set schema geocoding_${YEAR};
alter table address_to_geo_ab_${YEAR} set schema geocoding_${YEAR};
alter table address_to_geo_ac_${YEAR} set schema geocoding_${YEAR};
alter table address_to_geo_ad_${YEAR} set schema geocoding_${YEAR};
alter table address_to_geo_ae_${YEAR} set schema geocoding_${YEAR};


EOF

pg_dump --no-owner nfirs_dev > firecares_nfirs_7_14_2015
gzip firecares_nfirs_7_14_2015 > firecares_nfirs_7_14_2015.gz
