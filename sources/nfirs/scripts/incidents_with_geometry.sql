/*

# Find incidents by department given the FDID and State abbreviation #

Returns incidents for the fire department including lookup code descriptions and the first matching address
geometry from the incident address table.

Enter values for the FDID and STATE.

*/

SELECT bi.state, bi.fdid, bi.inc_date, bi.inc_no, bi.exp_no, bi.version, bi.dept_sta, bi.inc_type,inc_type.code_descr "inc_type_desc",
 bi.add_wild,bi.aid, aid.code_descr "aid_desc", bi.alarm, bi.arrival, bi.inc_cont, bi.lu_clear, bi.shift, bi.alarms, bi.district, bi.act_tak1,
act_tak1.code_descr "act_tak1_desc", bi.act_tak2, act_tak2.code_descr "act_tak2_desc", bi.act_tak3,
act_tak3.code_descr "act_tak3_desc", bi.app_mod, bi.sup_app, bi.ems_app, bi.oth_app, bi.sup_per, bi.ems_per, bi.oth_per,
bi.resou_aid, bi.prop_loss, bi.cont_loss, bi.prop_val, bi.cont_val, bi.ff_death, bi.oth_death, bi.ff_inj, bi.oth_inj,
bi.det_alert, bi.haz_rel, bi.mixed_use, bi.prop_use, prop_use.code_descr "prop_use_desc", bi.census,
(SELECT geom FROM incidentaddress WHERE state=bi.state AND fdid=bi.fdid AND inc_date=bi.inc_date AND inc_no=bi.inc_no AND geom IS NOT NULL LIMIT 1) "address"
FROM basicincident bi
LEFT JOIN (SELECT code_value, code_descr from codelookup where fieldid='ACT_TAK1') AS act_tak1 ON bi.act_tak1=act_tak1.code_value
LEFT JOIN (SELECT code_value, code_descr from codelookup where fieldid='ACT_TAK2') AS act_tak2 ON bi.act_tak2=act_tak2.code_value
LEFT JOIN (SELECT code_value, code_descr from codelookup where fieldid='ACT_TAK3') AS act_tak3 ON bi.act_tak3=act_tak3.code_value
LEFT JOIN (SELECT code_value, code_descr from codelookup where fieldid='INC_TYPE') AS inc_type ON bi.inc_type=inc_type.code_value
LEFT JOIN (SELECT code_value, code_descr from codelookup where fieldid='AID') AS aid ON bi.aid=aid.code_value
LEFT JOIN (SELECT code_value, code_descr from codelookup where fieldid='PROP_USE') AS prop_use ON bi.prop_use=prop_use.code_value;
-- WHERE fdid='' AND STATE='';
