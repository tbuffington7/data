/*

# Filter NFIRS to only building fires starting from PDR format files #

Aid
---
1 Mutual aid received
2 Automatic aid received
3 Mutual aid given
4 Automatic aid given
5 Other aid given
N None

Incident Type
-------------
111 Building Fire
112 Fires in structures other than in a building
113 Cooking fire, confined to container
114 Chimney or flue fire, confined to chimney or flue
115 Incinerator overload or malfunction, fire confined
116 Fuel burner/boiler malfunction, fire confined
117 Commercial Compactor fire, confined to rubbish
118 Trash or rubbish fire, contained
120 Fire in mobile prop. used as a fixed struc., other
121 Fire in mobile home used as fixed residence
122 Fire in motor home, camper, recreational vehicle
123 Fire in portable building, fixed location

Structure Type
--------------
1 Enclosed building
2 Fixed portable or mobile structure

*/

CREATE TABLE tempfire AS
  SELECT * FROM fireincident
  WHERE version='5.0';

ALTER TABLE tempfire DROP COLUMN version;
ALTER TABLE tempfire ADD PRIMARY KEY (state, fdid, inc_date, inc_no, exp_no);

CREATE TABLE buildingfires AS
  SELECT * FROM basicincident b
  INNER JOIN tempfire f USING (state, fdid, inc_date, inc_no, exp_no)
  WHERE b.aid NOT IN ('3','4') AND (((b.inc_date BETWEEN '20020101' AND '20071231' AND b.inc_type IN ('111','112','120','121','122','123')) OR (b.inc_date BETWEEN '20080101' AND '20121231' AND b.inc_type IN ('111','120','121','122','123')) AND f.struc_type IN ('1','2')) OR b.inc_type IN ('113','114','115','116','117','118') AND (f.struc_type IN ('1','2') OR f.struc_type IS NULL));

DROP TABLE tempfire;
