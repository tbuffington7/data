SELECT area_orig,
       first_ign,
       x.cnt
FROM
    ( SELECT *,
             row_number() over (partition BY area_orig
                                ORDER BY area_orig, w.cnt DESC, first_ign) row_num
     FROM
         (SELECT bf.area_orig,
                 bf.first_ign,
                 count(*) OVER ( PARTITION BY bf.area_orig, bf.first_ign ) AS cnt,
                 row_number() OVER ( PARTITION BY bf.area_orig, bf.first_ign ) AS row_numbers
          FROM buildingfires bf
          WHERE bf.area_orig IN
                  ( SELECT area_orig
                   FROM buildingfires
                   WHERE prop_use = '449'
                       AND area_orig != 'UU'
                   GROUP BY area_orig
                   ORDER BY count(1) DESC LIMIT 8)
              AND bf.prop_use = '449'
              AND bf.first_ign != 'UU'
          ORDER BY area_orig,
                   first_ign ) w
     WHERE w.row_numbers = 1) x
WHERE x.row_num < 7
ORDER BY area_orig,
         x.cnt DESC,
         first_ign

-- Sanity checks

select area_orig, first_ign, count(1)
from buildingfires
group by area_orig, first_ign
order by count desc
