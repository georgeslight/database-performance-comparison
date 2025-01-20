SELECT
    betriebstag,
    SUM(
            CASE
                WHEN tu_delta > 0
                    THEN 1
                ELSE 0
            END
    ) AS punctual_trips,
    SUM(
            CASE
                WHEN tu_delta <0
                    THEN 1
                ELSE 0
            END
    ) AS delayed_trips
FROM
    qdaba.puenkt
GROUP BY
    betriebstag
ORDER BY
    betriebstag;


SELECT count(*) from puenkt WHERE tu_delta > 0;