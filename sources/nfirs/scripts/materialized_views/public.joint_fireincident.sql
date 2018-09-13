CREATE MATERIALIZED VIEW joint_fireincident AS
SELECT * FROM public.fireincident fi
  WHERE (state, fdid, inc_date, inc_no, exp_no)
    NOT IN (select state, fdid, inc_date, inc_no, exp_no FROM departme
UNION ALL
SELECT * FROM department_nfirs.fireincident dfi;
    
CREATE UNIQUE INDEX ON joint_fireincident (state, fdid, inc_date, inc_no, exp_no);
