/*

# Count of residential fires grouped by fire spread #

Property Use
------------
419 1- or 2-family dwelling, detached, manufactured home, mobile home not in transit, duplex.
429 Multifamily dwelling. Includes apartments, condos, townhouses, rowhouses, tenements.
439 Boarding/Rooming house. Includes residential hotels and shelters.
449 Hotel/Motel, commercial.
459 Residential board and care. Includes long-term care facilities, halfway houses, and assisted-care housing facilities. Excludes nursing facilities (311).
460 Dormitory-type residence, other.
462 Sorority house, fraternity house.
464 Barracks, dormitory. Includes nurses’ quarters, military barracks, monastery/convent dormitories, bunk houses, workers’ barracks.
400 Residential, other

(From: http://www.dps.state.ia.us/fm/main/pdf/NFIRS/2013/US%20Fire%20Admin%20Trng%20Resources/Basic%20Module/Property%20Use.pdf)

Fire Spread
-----------
1 Confined to object of origin
2 Confined to room of origin
3 Confined to floor of origin
4 Confined to building of origin
5 Beyond building of origin

Note: This approach throws away building fires for which the fire_sprd field is not recorded, about 228,000 fires are lost this way,
out of roughly 1.5 million

*/

# National
SELECT fire_sprd, count(*) AS fires
FROM buildingfires
WHERE prop_use IN ('419','429','439','449','459','460','462','464','400') AND fire_sprd IN ('1','2','3','4','5')
GROUP BY fire_sprd
ORDER BY fire_sprd;

# Individual Departments
SELECT fdid, fire_sprd, count(*) AS fires
FROM buildingfires
WHERE prop_use IN ('419','429','439','449','459','460','462','464','400') AND fire_sprd IN ('1','2','3','4','5')
GROUP BY fdid, fire_sprd
ORDER BY fdid, fire_sprd;
