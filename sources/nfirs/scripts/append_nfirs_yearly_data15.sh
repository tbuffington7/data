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


alter table geocoding_2015.geocoded_addresses alter column zip5 type character varying(5);
update geocoding_2015.geocoded_addresses set zip5 = lpad(zip5, 5, '0');
update geocoding_2015.geocoded_addresses set apt_no = '' where apt_no is null;


create table public.arson_bk as select * from public.arson;
create table public.arsonagencyreferal_bk as select * from public.arsonagencyreferal;
create table public.arsonjuvsub_bk as select * from public.arsonjuvsub;
create table public.basicaid_bk as select * from public.basicaid;
create table public.incidentaddress_bk as select * from public.incidentaddress;
create table public.fireincident_bk as select * from public.fireincident;
create table public.ems_bk as select * from public.ems;
create table public.wildlands_bk as select * from public.wildlands;
create table public.civiliancasualty_bk as select * from public.civiliancasualty;
create table public.fdheader_bk as select * from public.fdheader;
create table public.hazmobprop_bk as select * from public.hazmobprop;
create table public.ffcasualty_bk as select * from public.ffcasualty;
create table public.ffequipfail_bk as select * from public.ffequipfail;
create table public.basicincident_bk as select * from public.basicincident;
create table public.hazchem_bk as select * from public.hazchem;
create table public.hazmat_bk as select * from public.hazmat;
create table public.hazmatequipinvolved_bk as select * from public.hazmatequipinvolved;


-- 605099 pre - 643442 post
INSERT INTO public.arson_bk(
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
  FROM geocoding_2015.arson);

-- 318630 pre - 324018 post
INSERT INTO public.arsonagencyreferal_bk(
            state, fdid, inc_date, inc_no, exp_no, agency_nam, version, ag_st_num,
            ag_st_pref, ag_street, ag_st_type, ag_st_suff, ag_apt_no, ag_city,
            ag_state, ag_zip5, ag_zip4, ag_phone, ag_case_no, ag_ori, ag_fid,
            ag_fdid)
    (SELECT state, fdid, inc_date, inc_no, exp_no, agency_nam, version, ag_st_num,
       ag_st_pref, ag_street, ag_st_type, ag_st_suff, ag_apt_no, ag_city,
       ag_state, ag_zip5, ag_zip4, ag_phone, ag_case_no, ag_ori, ag_fid,
       ag_fdid
  FROM geocoding_2015.arsonagencyreferal);

-- 505154 pre - 509462 post
INSERT INTO public.arsonjuvsub_bk(
            state, fdid, inc_date, inc_no, exp_no, sub_seq_no, version, age,
            gender, race, ethnicity, fam_type, risk_fact1, risk_fact2, risk_fact3,
            risk_fact4, risk_fact5, risk_fact6, risk_fact7, risk_fact8, juv_dispo)
    (SELECT state, fdid, inc_date, inc_no, exp_no, sub_seq_no, version, age,
       gender, race, ethnicity, fam_type, risk_fact1, risk_fact2, risk_fact3,
       risk_fact4, risk_fact5, risk_fact6, risk_fact7, risk_fact8, juv_dispo
  FROM geocoding_2015.arsonjuvsub);

-- 3532902 pre - 3792354 post
INSERT INTO public.basicaid_bk(
            state, fdid, inc_date, inc_no, exp_no, nfir_ver, fdidrecaid,
            fdidstrec, inc_nofdid)
    (SELECT state, fdid, inc_date, inc_no, exp_no, nfir_ver, fdidrecaid,
       fdidstrec, inc_nofdid
  FROM geocoding_2015.basicaid);

-- 152126 pre - 163482 post
INSERT INTO public.civiliancasualty_bk(
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
  FROM geocoding_2015.civiliancasualty);

-- 6622 pre - 6622 post (appears to be a static table, same existing and same incoming, replacing EVERYTHING w/ the new values)
delete from codelookup;
INSERT INTO codelookup(
            fieldid, code_value, code_descr)
    (SELECT fieldid, code_value, code_descr
  FROM geocoding_2015.codelookup);

-- 1294841 pre - 1298950 post
INSERT INTO public.ems_bk(
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
  FROM geocoding_2015.ems);

-- 39494 pre - 39632 post (<20 dupes)
INSERT INTO public.fdheader_bk(
            state, fdid, fd_name, fd_str_no, fd_str_pre, fd_street, fd_str_typ,
            fd_str_suf, fd_city, fd_zip, fd_phone, fd_fax, fd_email, fd_fip_cty,
            no_station, no_pd_ff, no_vol_ff, no_vol_pdc)
    (SELECT distinct on (state, fdid) state, fdid, fd_name, fd_str_no, fd_str_pre, fd_street, fd_str_typ,
       fd_str_suf, fd_city, fd_zip, fd_phone, fd_fax, fd_email, fd_fip_cty,
       no_station, no_pd_ff, no_vol_ff, no_vol_pdc
  FROM geocoding_2015.fdheader where (state, fdid) not in (select state, fdid from public.fdheader));

-- 118027 pre - 127125 post
INSERT INTO public.ffcasualty_bk(
            state, fdid, inc_date, inc_no, exp_no, ff_seq_no, version, gender,
            career, age, inj_date, responses, assignment, phys_cond, severity,
            taken_to, activity, symptom, pabi, cause, factor, object, wio,
            relation, story, location, vehicle, prot_eqp)
    (SELECT state, fdid, inc_date, inc_no, exp_no, ff_seq_no, version, gender,
       career, age, inj_date, responses, assignment, phys_cond, severity,
       taken_to, activity, symptom, pabi, cause, factor, object, wio,
       relation, story, location, vehicle, prot_eqp
  FROM geocoding_2015.ffcasualty);

-- 35588 pre - 36080 post
INSERT INTO public.ffequipfail_bk(
            state, fdid, inc_date, inc_no, exp_no, cas_seq_no, eqp_seq_no,
            version, equip_item, eqp_prob, eqp_man, eqp_mod, eqp_ser_no)
    (SELECT state, fdid, inc_date, inc_no, exp_no, cas_seq_no, eqp_seq_no,
       version, equip_item, eqp_prob, eqp_man, eqp_mod, eqp_ser_no
  FROM geocoding_2015.ffequipfail);

-- 40635486 pre - 42795807 post
INSERT INTO public.basicincident_bk(
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
  FROM geocoding_2015.basicincident);


-- 7752465 pre - 8351826 post (many dups in fireincident_2013 ~500k)
INSERT INTO public.fireincident_bk(
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
  FROM geocoding_2015.fireincident);


UPDATE public.fireincident set fire_sprd=null where fire_sprd='';

-- 98271 pre - 107260 post
INSERT INTO public.hazchem_bk(
            state, fdid, inc_date, inc_no, exp_no, seq_number, version, un_number,
            dot_class, cas_regis, chem_name, cont_type, cont_cap, cap_unit,
            amount_rel, units_rel, phys_state, rel_into)
    (SELECT state, fdid, inc_date, inc_no, exp_no, seq_number, version, un_number,
       dot_class, cas_regis, chem_name, cont_type, cont_cap, cap_unit,
       amount_rel, units_rel, phys_state, rel_into
  FROM geocoding_2015.hazchem)

-- 133177 pre - 145465 post
INSERT INTO public.hazmat_bk(
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
  FROM geocoding_2015.hazmat);

-- 38258 pre - 50546 post
INSERT INTO public.hazmatequipinvolved_bk(
            state, fdid, inc_date, inc_no, exp_no, version, eq_brand, eq_model,
            eq_ser_no, eq_year)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, eq_brand, eq_model,
       eq_ser_no, eq_year
  FROM geocoding_2015.hazmatequipinvolved);

-- 58210 pre - 70498 post
INSERT INTO public.hazmobprop_bk(
            state, fdid, inc_date, inc_no, exp_no, version, mp_type, mp_make,
            mp_model, mp_year, mp_license, mp_state, mp_dot_icc)
    (SELECT state, fdid, inc_date, inc_no, exp_no, version, mp_type, mp_make,
       mp_model, mp_year, mp_license, mp_state, mp_dot_icc
  FROM geocoding_2015.hazmobprop);


-- 728789 pre - 676124 post
INSERT INTO public.wildlands_bk(
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
  FROM geocoding_2015.wildlands);

select column_name, data_type, character_maximum_length from public.columns where table_name='public.incidentaddress_bk'

update geocoding_2015.geocoded_addresses
set city= replace(city, ', Town of', '')
update geocoding_2015.geocoded_addresses
set city= replace(city, ', Village of', '')

UPDATE geocoding_2015.geocoded_addresses SET city = substring(city from 1 for 20) where length(city) > 20;
update geocoding_2015.geocoded_addresses set inc_no = 1000000 where inc_no = 10000000;
update geocoding_2015.geocoded_addresses set num_mile = 99999999 where num_mile > 99999999;

-- 40635500 pre - 42795821 post
INSERT INTO public.incidentaddress_bk(
            state, fdid, inc_date, inc_no, exp_no, loc_type, num_mile, street_pre,
            streetname, streettype, streetsuf, apt_no, city, state_id, zip5,
            zip4, geom)
    (SELECT state, fdid, inc_date, inc_no, exp_no, loc_type, num_mile, street_pre,
       streetname, streettype, streetsuf, apt_no, city, state_id, zip5,
       zip4, wkb_geometry
  FROM geocoding_2015.geocoded_addresses);

