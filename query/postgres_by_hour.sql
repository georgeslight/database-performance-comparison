SELECT
    p.betriebstag AS DAY,
    EXTRACT(
        HOUR
        FROM
            p.soll_tu
    ) AS hour_of_day, -- Extracts the hour (00-23)
    COUNT(p.tu_delta) AS total_trips,
    ROUND(
        100.0 * SUM(
            CASE
                WHEN p.tu_delta IS NOT NULL
                AND ABS(p.tu_delta) <= 60 THEN 1
                ELSE 0
            END
        ) / COUNT(p.tu_delta),
        2
    ) AS on_time_prcnt
FROM
    (
        SELECT
            p_1.betriebstag,
            p_1.soll_tu,
            CASE
                WHEN p_1.tu_delta_raw < 43200 THEN p_1.tu_delta_raw
                ELSE p_1.tu_delta_raw - 86400
            END AS tu_delta
        FROM
            (
                SELECT
                    p_2.betriebstag,
                    p_2.soll_tu,
                    EXTRACT(
                        EPOCH
                        FROM
                            (p_2.ist_tu - p_2.soll_tu)
                    ) AS tu_delta_raw
                FROM
                    qdaba.puenkt p_2
                WHERE
                    EXTRACT(
                        MONTH
                        FROM
                            p_2.betriebstag
                    ) = 1 -- Select trips only in JAN
            ) p_1
    ) p
GROUP BY
    DAY,
    hour_of_day
ORDER BY
    DAY,
    hour_of_day;