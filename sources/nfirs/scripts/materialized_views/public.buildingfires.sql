CREATE MATERIALIZED VIEW IF NOT EXISTS public.buildingfires AS
  SELECT b.*, f.num_unit, f.not_res, f.bldg_invol, f.acres_burn, f.less_1acre, f.on_site_m1, f.mat_stor1, f.on_site_m2,
      f.mat_stor2, f.on_site_m3, f.mat_stor3, f.area_orig, f.heat_sourc, f.first_ign, f.conf_orig, f.type_mat, f.cause_ign,
      f.fact_ign_1, f.fact_ign_2, f.hum_fac_1, f.hum_fac_2, f.hum_fac_3, f.hum_fac_4, f.hum_fac_5, f.hum_fac_6, f.hum_fac_7,
      f.hum_fac_8, f.age, f.sex, f.equip_inv, f.sup_fac_1, f.sup_fac_2, f.sup_fac_3, f.mob_invol, f.mob_type, f.mob_make, f.mob_model,
      f.mob_year, f.mob_lic_pl, f.mob_state, f.mob_vin_no, f.eq_brand, f.eq_model, f.eq_ser_no, f.eq_year, f.eq_power, f.eq_port,
      f.fire_sprd, f.struc_type, f.struc_stat, f.bldg_above, f.bldg_below, f.bldg_lgth, f.bldg_width, f.tot_sq_ft, f.fire_orig,
      f.st_dam_min, f.st_dam_sig, f.st_dam_hvy, f.st_dam_xtr, f.flame_sprd, f.item_sprd, f.mat_sprd, f.detector, f.det_type,
      f.det_power, f.det_operat, f.det_effect, f.det_fail, f.aes_pres, f.aes_type, f.aes_oper, f.no_spr_op, f.aes_fail
  FROM basicincident b
    INNER JOIN fireincident f
      USING (state, fdid, inc_date, inc_no, exp_no)
    WHERE
      f.version='5.0'
      AND
      (
        b.aid NOT IN ('3','4')
        AND (
          (
            (b.inc_date BETWEEN '20020101' AND '20071231' AND b.inc_type IN ('111','112','120','121','122','123'))
            OR (
              b.inc_date >= '20080101' AND b.inc_type IN ('111','120','121','122','123')
            )
            AND f.struc_type IN ('1','2')
          )
          OR
            b.inc_type IN ('113','114','115','116','117','118')
          AND (
            f.struc_type IN ('1','2') OR f.struc_type IS NULL
          )
        )
      );
