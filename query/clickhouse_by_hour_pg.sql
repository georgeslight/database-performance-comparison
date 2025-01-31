SELECT
    p.betriebstag AS DAY,
    formatDateTime (p.soll_tu, '%H') AS hour_of_day, -- Extracts the hour (00-23)
    COUNT(p.tu_delta) AS total_trips,
    ROUND(
        100.0 * countIf (
            p.tu_delta IS NOT NULL
            AND ABS(p.tu_delta) <= 60
        ) / COUNT(p.tu_delta),
        2
    ) AS on_time_prcnt
FROM
    (
        SELECT
            p_1.betriebstag,
            p_1.soll_tu,
            if (
                p_1.tu_delta_raw < 43200,
                p_1.tu_delta_raw,
                p_1.tu_delta_raw - 86400
            ) AS tu_delta
        FROM
            (
                SELECT
                    p_2.betriebstag,
                    p_2.soll_tu,
                    toInt32 (dateDiff ('second', p_2.soll_tu, p_2.ist_tu)) AS tu_delta_raw
                FROM
                    qdaba.puenkt_pg p_2
                WHERE
                    formatDateTime (p_2.betriebstag, '%m') IN ('01') -- Select trips only in JAN
            ) p_1
    ) p
GROUP BY
    DAY,
    hour_of_day
ORDER BY
    DAY,
    hour_of_day;