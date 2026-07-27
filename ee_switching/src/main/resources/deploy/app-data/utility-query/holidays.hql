WITH holidays_fixed AS
  (SELECT concat('01/01/',${hiveconf:year}) AS DAY
   UNION ALL SELECT concat('06/01/',${hiveconf:year}) AS DAY
   UNION ALL SELECT concat('25/04/',${hiveconf:year}) AS DAY
   UNION ALL SELECT concat('01/05/',${hiveconf:year}) AS DAY
   UNION ALL SELECT concat('02/06/',${hiveconf:year}) AS DAY
   UNION ALL SELECT concat('15/08/',${hiveconf:year}) AS DAY
   UNION ALL SELECT concat('01/11/',${hiveconf:year}) AS DAY
   UNION ALL SELECT concat('08/12/',${hiveconf:year}) AS DAY
   UNION ALL SELECT concat('25/12/',${hiveconf:year}) AS DAY
   UNION ALL SELECT concat('26/12/',${hiveconf:year}) AS DAY),
     pasqua AS
  (SELECT CASE
              WHEN giorno = '26'
                   AND mese = '04' THEN '19'
              WHEN giorno = '25'
                   AND mese = '04'
                   AND d=28
                   AND e=6
                   AND a1>10 THEN '18'
              ELSE giorno
          END AS giorno,
          mese,
          anno
   FROM
     (SELECT CASE
                 WHEN d+e < 10 THEN lpad(cast(r_2 AS string),2,'0')
                 ELSE lpad(cast(r_1 AS string),2,'0')
             END AS giorno,
             CASE
                 WHEN d+e < 10 THEN '03'
                 ELSE '04'
             END AS Mese , ${hiveconf:year} AS Anno,
                                          d,
                                          e,
                                          a1
      FROM
        (SELECT (d+e-9) AS r_1,
                (d+e+22) AS r_2,
                d,
                e,
                a1
         FROM
           (SELECT d,
                   ((2*b1)+(4*c1)+(6*d)+n)%7 AS e,
                   a1
            FROM
              (SELECT ((19*a1)+m)%30 AS d,
                      a1,
                      b1,
                      c1,
                      n
               FROM
                 (SELECT anno%a AS a1,
                         anno%b AS b1,
                         anno%c AS c1,
                         m,
                         n
                  FROM
                    (SELECT ${hiveconf:year} AS anno,
                                           19 AS a,
                                           4 AS b,
                                           7 AS c,
                                           24 AS m,
                                           5 AS n) x) y) z) w) h) j)

SELECT *
FROM holidays_fixed
UNION ALL
SELECT concat(giorno,'/',mese,'/',anno) AS DAY
FROM pasqua
UNION ALL
SELECT concat(CASE
                  WHEN (cast(giorno AS int)+1) <10 THEN concat('0',(cast(giorno AS int)+1))
                  ELSE (cast(giorno AS int)+1)
              END,'/',mese,'/',anno) AS DAY
FROM pasqua;